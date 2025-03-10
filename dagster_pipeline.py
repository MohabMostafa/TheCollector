import os
import random
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import yt_dlp
import yaml
import requests
import time
from pydub import AudioSegment
from natsort import natsorted

from dagster import (
    asset,
    OpExecutionContext,
    Definitions,
    sensor,
    RunRequest,
    job,
    DefaultSensorStatus,
)

# File paths for pipeline and state
KEYWORDS_FILE = "keywords/keywords.txt"
URLS_FILE = "url_list/urls.txt"
CONFIG_FILE = "config.yaml"         # Contains max_results, lang, country
PROXIES_JSON_FILE = "proxies.json"    # Maps country to list of proxies
VIDEOS_INFO_JSON = "url_list/videos_info.json"
STATE_FILE_PATH = "url_list/search_states.json"  # Holds state for keywords
LID_BASE_URL = "http://localhost:3002"

# ---------------- Configuration and State Functions ----------------
def load_pipeline_config():
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)

def load_proxies_json():
    with open(PROXIES_JSON_FILE, "r") as f:
        return json.load(f)

def load_search_states(state_file_path):
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r') as f:
            return json.load(f)
    return {}

def save_search_states(state_file_path, states):
    with open(state_file_path, 'w') as f:
        json.dump(states, f, ensure_ascii=False, indent=4)

def save_progress(file_path, json_path, existing_urls, videos_info, total_duration):
    with open(file_path, 'w') as f:
        f.writelines(f"{url}\n" for url in existing_urls)
    with open(json_path, 'w') as f:
        json.dump({
            'videos': videos_info,
            'total_duration': total_duration,
            'videos_count': len(videos_info)
        }, f, ensure_ascii=False, indent=4)

# ---------------- Text Processing Helpers ----------------
def contains_arabic(text):
    arabic_range = re.compile('[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+')
    return bool(arabic_range.search(text))

def has_lang_subtitles(entry, lang):
    subtitles = entry.get('subtitles', {})
    lang = lang.lower()
    return lang in subtitles or any(lang in key.lower() for key in subtitles.keys())

# ---------------- File I/O Helpers ----------------
def read_keywords():
    if not os.path.exists(KEYWORDS_FILE):
        return []
    with open(KEYWORDS_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]

def read_existing_urls():
    if os.path.exists(URLS_FILE):
        with open(URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

# ---------------- Optimized Keyword Processing Functions ----------------
def process_keyword(keyword, proxies, ydl_opts, existing_urls, lock, states, state_file_path, max_results):
    new_videos_info = []
    total_duration = 0
    ydl_opts['geo_verification_proxy'] = random.choice(proxies) if proxies else None
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            result = ydl.extract_info(f"ytsearch{max_results}:{keyword}", download=False)
            for entry in result.get('entries', []):
                url = entry.get('webpage_url')
                if not url or url in existing_urls or not has_lang_subtitles(entry, ydl_opts.get('subtitleslangs', ['ar'])[0]):
                    continue
                title = entry.get('title', '')
                duration = entry.get('duration', 0)
                if not contains_arabic(title) or "مترجم" in title or 'Music' in entry.get('categories', []) or duration < 60:
                    continue
                with lock:
                    if url not in existing_urls:
                        new_videos_info.append({'title': title, 'url': url, 'duration': duration})
                        total_duration += duration
                        existing_urls.add(url)
            with lock:
                states[keyword] = "done"
                save_search_states(state_file_path, states)
        except Exception as e:
            print(f"Error processing keyword '{keyword}': {e}")
    return new_videos_info, total_duration

def process_keywords_and_update_json(keywords, file_path, json_path, proxies, state_file_path, max_results):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    existing_urls = set()
    videos_info = []
    total_duration = 0
    lock = Lock()
    states = load_search_states(state_file_path)
    
    if os.path.isfile(file_path):
        with open(file_path, 'r') as f:
            existing_urls.update(f.read().splitlines())
    
    if os.path.isfile(json_path):
        with open(json_path, 'r') as f:
            data = json.load(f)
            videos_info = data.get('videos', [])
            total_duration = data.get('total_duration', 0)
            existing_urls.update(video['url'] for video in videos_info)
    
    ydl_opts = {
        'skip_download': True,
        'quiet': False,
        'dateafter': "20200101",
        'subtitleslangs': [load_pipeline_config().get("lang", "ar")],
    }
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for keyword in keywords:
            if states.get(keyword) is None:
                states[keyword] = "in progress"
                futures[executor.submit(process_keyword, keyword, proxies, ydl_opts, existing_urls, lock, states, state_file_path, max_results)] = keyword
        
        save_search_states(state_file_path, states)
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Keywords"):
            kw = futures[future]
            try:
                new_videos, duration = future.result()
                videos_info.extend(new_videos)
                total_duration += duration
                save_progress(file_path, json_path, existing_urls, videos_info, total_duration)
            except Exception as exc:
                print(f"{kw} generated an exception: {exc}")
    print(f"Updated total duration: {total_duration} seconds")

# ---------------- Dagster Assets ----------------
@asset
def download_audio_and_captions(context: OpExecutionContext):
    """Asset that processes URL files in 'url_list' and downloads audio and subtitles for videos that have target subtitles."""
    config = load_pipeline_config()
    target_lang = config.get("lang", "ar")
    folder_path = os.path.join(os.getcwd(), "url_list")
    file_list = [f for f in os.listdir(folder_path) if f.endswith(".txt")]
    context.log.info(f"Found {len(file_list)} URL file(s) in {folder_path}")
    count_total = 0
    
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        context.log.info(f"Processing file: {file_path}")
        with open(file_path, "r") as file:
            urls = [line.strip() for line in file if line.strip()]
        
        count_urls_with_cc = 0
        for url in urls:
            context.log.info(f"Processing URL: {url}")
            if check_lang_captions(url, target_lang):
                context.log.info(f"{url} has target subtitles ({target_lang})")
                video_id = get_video_id(url)
                if not video_id:
                    context.log.info(f"Could not extract video ID for {url}")
                    continue
                if not check_file_existence(video_id, target_lang):
                    download_lang_captions(url, target_lang)
                else:
                    context.log.info(f"Skipping: already downloaded {video_id}")
                count_urls_with_cc += 1
        context.log.info(f"Finished processing file: {file_path}")
        context.log.info(f"Videos with target subtitles in this file: {count_urls_with_cc} out of {len(urls)}")
        count_total += count_urls_with_cc
    context.log.info(f"Total videos with target subtitles across all files: {count_total}")

@asset(deps=[download_audio_and_captions])
def filter_song_urls(context: OpExecutionContext):
    """Asset that filters out song URLs from URLS_FILE."""
    file_path = URLS_FILE
    if not os.path.exists(file_path):
        context.log.info(f"{file_path} does not exist. Nothing to filter.")
        return []
    
    with open(file_path, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    def is_song(url):
        try:
            result = subprocess.run(["yt-dlp", "-j", url], capture_output=True, text=True)
            video_info = json.loads(result.stdout)
            categories_to_check = ["music", "entertainment"]
            keywords_to_check = ["official music video", "lyric video", "audio"]
            if "categories" in video_info:
                if any(category.lower() in categories_to_check for category in video_info["categories"]):
                    return True
            if "tags" in video_info:
                if any("music" in tag.lower() for tag in video_info["tags"]):
                    return True
            title_description = video_info.get("title", "").lower() + " " + video_info.get("description", "").lower()
            if any(keyword in title_description for keyword in keywords_to_check):
                return True
        except Exception as e:
            context.log.error(f"Error processing {url}: {e}")
        return False

    not_songs = []
    for url in urls:
        context.log.info(f"Filtering URL: {url}")
        if not is_song(url):
            not_songs.append(url)
        else:
            context.log.info(f"Removing {url} as it is likely a song.")
    
    with open(file_path, 'w') as f:
        for url in not_songs:
            f.write(url + "\n")
    context.log.info(f"Filtered out {len(urls) - len(not_songs)} song URLs out of {len(urls)}.")
    return not_songs

@asset(deps=[filter_song_urls])
def optimized_youtube_keyword_processor(context: OpExecutionContext):
    """Asset that uses optimized, concurrent processing to search for video URLs."""
    config = load_pipeline_config()
    max_results = config.get("max_results", 10)
    lang = config.get("lang", "ar")
    country = config.get("country", "egypt")
    proxies_data = load_proxies_json()
    proxies = proxies_data.get(country, [])
    
    file_path = URLS_FILE
    json_path = VIDEOS_INFO_JSON
    state_file_path = STATE_FILE_PATH
    
    if not os.path.exists(KEYWORDS_FILE):
        context.log.info("No keywords file found.")
        return
    
    with open(KEYWORDS_FILE, "r") as f:
        keywords = [line.strip() for line in f if line.strip()]
    
    context.log.info(f"Starting optimized processing for keywords: {keywords}")
    process_keywords_and_update_json(keywords, file_path, json_path, proxies, state_file_path, max_results)
    context.log.info("Optimized keyword processing completed.")

# ---------------- Language Detection Client ----------------
@asset(deps=[optimized_youtube_keyword_processor])
def language_detection_client(context: OpExecutionContext):
    """
    Asset that checks the health of the language detection server,
    starts processing, and polls until processing is complete.
    """

    config = load_pipeline_config()
    LID_BASE_URL = config.get("LID_BASE_URL", "http://lang_detector:3002")

    def check_health(wait_for_model=False, timeout=300):
        start_time = time.time()
        while True:
            try:
                response = requests.get(f"{LID_BASE_URL}/health", timeout=10)
                response_data = response.json()

                if response.status_code == 200:
                    # If waiting for model, check if it's still initializing
                    if wait_for_model and response_data.get("status") == "initializing":
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for model initialization ({timeout} seconds)")
                            return False
                        context.log.info(f"Model is still initializing. Waiting... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue

                    model_loaded = response_data.get("model_loaded", False)
                    context.log.info(f"Server is healthy! Model loaded: {model_loaded}")

                    if wait_for_model and not model_loaded:
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for model to load ({timeout} seconds)")
                            return False
                        context.log.info(f"Waiting for model to load... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue

                    return True
                else:
                    context.log.error(f"Server health check failed with status code: {response.status_code}")
                    if wait_for_model:
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for server ({timeout} seconds)")
                            return False
                        context.log.info(f"Retrying in 5 seconds... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue
                    return False

            except requests.exceptions.RequestException as e:
                context.log.error(f"Error connecting to server: {e}")
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    context.log.error(f"Timeout waiting for server connection ({timeout} seconds)")
                    return False
                context.log.info(f"Retrying connection in 5 seconds... ({elapsed:.1f}s)")
                time.sleep(5)

    def start_processing():
        try:
            response = requests.post(f"{LID_BASE_URL}/process", timeout=30)
            data = response.json()
            context.log.info(f"Process start response: {json.dumps(data, indent=2)}")
            return data
        except Exception as e:
            context.log.error(f"Error starting processing: {e}")
            return None

    def check_status():
        try:
            response = requests.get(f"{LID_BASE_URL}/status", timeout=30)
            return response.json()
        except Exception as e:
            context.log.error(f"Error checking status: {e}")
            return None

    # Execute the client pipeline steps:
    if not check_health(wait_for_model=True, timeout=600):
        context.log.error("Server is not healthy or timeout reached. Exiting asset.")
        return {"status": "failed"}

    # Start processing unless you want to skip it (similar to --skip-process)
    result = start_processing()
    if result and result.get("status") == "error":
        context.log.error(f"Processing error: {result.get('message')}")
        return {"status": "error", "message": result.get("message")}

    context.log.info("Polling processing status...")
    while True:
        status = check_status()
        if status is None:
            context.log.error("Error checking status. Exiting asset.")
            return {"status": "error", "message": "Error checking status"}
        if status.get("status") == "processing":
            context.log.info("Processing is still running. Waiting 5 seconds...")
            time.sleep(5)
        else:
            context.log.info("Processing completed!")
            context.log.info(f"Final status: {json.dumps(status, indent=2)}")
            break

    return status

# ---------------- Dialect Detection Client ----------------
@asset(deps=[language_detection_client])
def dialect_detection_client(context: OpExecutionContext):
    """
    Asset that checks the health of the dialect detection server,
    starts processing, and polls until processing is complete.
    """
    
    config = load_pipeline_config()
    DIALECT_BASE_URL = config.get("DIALECT_BASE_URL", "http://dialect_detector:3003")
    
    def check_health(wait_for_model=False, timeout=300):
        start_time = time.time()
        while True:
            try:
                response = requests.get(f"{DIALECT_BASE_URL}/health", timeout=10)
                response_data = response.json()
    
                if response.status_code == 200:
                    # If waiting for model, check if it's still initializing
                    if wait_for_model and response_data.get("status") == "initializing":
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for model initialization ({timeout} seconds)")
                            return False
                        context.log.info(f"Model is still initializing. Waiting... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue
    
                    model_loaded = response_data.get("model_loaded", False)
                    context.log.info(f"Server is healthy! Model loaded: {model_loaded}")
    
                    if wait_for_model and not model_loaded:
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for model to load ({timeout} seconds)")
                            return False
                        context.log.info(f"Waiting for model to load... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue
    
                    return True
                else:
                    context.log.error(f"Server health check failed with status code: {response.status_code}")
                    if wait_for_model:
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            context.log.error(f"Timeout waiting for server ({timeout} seconds)")
                            return False
                        context.log.info(f"Retrying in 5 seconds... ({elapsed:.1f}s)")
                        time.sleep(5)
                        continue
                    return False
    
            except requests.exceptions.RequestException as e:
                context.log.error(f"Error connecting to server: {e}")
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    context.log.error(f"Timeout waiting for server connection ({timeout} seconds)")
                    return False
                context.log.info(f"Retrying connection in 5 seconds... ({elapsed:.1f}s)")
                time.sleep(5)
    
    def start_processing():
        try:
            response = requests.post(f"{DIALECT_BASE_URL}/process", timeout=30)
            data = response.json()
            context.log.info(f"Process start response: {json.dumps(data, indent=2)}")
            return data
        except Exception as e:
            context.log.error(f"Error starting processing: {e}")
            return None
    
    def check_status():
        try:
            response = requests.get(f"{DIALECT_BASE_URL}/status", timeout=30)
            return response.json()
        except Exception as e:
            context.log.error(f"Error checking status: {e}")
            return None
    
    # Execute the client pipeline steps:
    if not check_health(wait_for_model=True, timeout=600):
        context.log.error("Server is not healthy or timeout reached. Exiting asset.")
        return {"status": "failed"}
    
    result = start_processing()
    if result and result.get("status") == "error":
        context.log.error(f"Processing error: {result.get('message')}")
        return {"status": "error", "message": result.get("message")}
    
    context.log.info("Polling processing status...")
    while True:
        status = check_status()
        if status is None:
            context.log.error("Error checking status. Exiting asset.")
            return {"status": "error", "message": "Error checking status"}
        if status.get("status") == "processing":
            context.log.info("Processing is still running. Waiting 5 seconds...")
            time.sleep(5)
        else:
            context.log.info("Processing completed!")
            context.log.info(f"Final status: {json.dumps(status, indent=2)}")
            break
    
    return status

@asset(deps=[dialect_detection_client])
def mixed_arabic_extractor(context: OpExecutionContext):
    """
    Asset to extract mixed language and Arabic-only transcriptions from VTT files 
    located in the audio-and-captions/Arabic/<dialect> folder. The dialect must 
    be provided in the op_config (valid options: "ECA", "MSA"). Files with no valid 
    transcriptions are deleted.
    """

    config = load_pipeline_config()
    lang = config.get("lang", "ar")
    dialect = config.get("dialect", "ECA")
    options = ["ECA", "MSA"]
    if dialect is None:
        context.log.error(
            "No dialect provided in configuration. Please provide one of: " + ", ".join(options)
        )
        return {"status": "failed", "message": "Missing dialect configuration"}
    if dialect not in options:
        context.log.error(f"Invalid dialect: {dialect}. Valid options: " + ", ".join(options))
        return {"status": "failed", "message": f"Invalid dialect: {dialect}"}

    lang = "ar"
    # Define regex patterns for Arabic and English characters
    arabic_pattern = re.compile(r"[\u0600-\u06FF]+")
    english_pattern = re.compile(r"[a-zA-Z]+")

    # Create output folders if they don't exist
    mixedlanguage_folder = os.path.join(os.getcwd(), f"mixedlanguage-{dialect}")
    arabic_only_folder = os.path.join(os.getcwd(), f"arabic-only-{dialect}")
    for folder in [mixedlanguage_folder, arabic_only_folder]:
        os.makedirs(folder, exist_ok=True)

    # Define the folder containing the VTT files for the given dialect
    audio_and_captions_folder = os.path.join(os.getcwd(), f"audio-and-captions/Arabic/{dialect}")
    if not os.path.exists(audio_and_captions_folder):
        context.log.error(f"Folder {audio_and_captions_folder} does not exist.")
        return {"status": "failed", "message": f"Folder {audio_and_captions_folder} not found"}

    vtt_files = [file for file in os.listdir(audio_and_captions_folder) if file.endswith(".vtt")]
    processed_count = 0
    deleted_count = 0
    skipped_count = 0

    with tqdm(total=len(vtt_files), desc="Processing files", unit="file") as pbar:
        for vtt_file in vtt_files:
            vtt_file_path = os.path.join(audio_and_captions_folder, vtt_file)
            mixedlanguage_file_path = os.path.join(mixedlanguage_folder, vtt_file[:-4] + "_mixedlanguage.vtt")
            arabic_only_file_path = os.path.join(arabic_only_folder, vtt_file[:-4] + "_arabic_only.vtt")

            # If both output files already exist, skip processing this file
            if os.path.exists(mixedlanguage_file_path) and os.path.exists(arabic_only_file_path):
                context.log.info(f"Skipping file '{vtt_file}' - output files already exist.")
                skipped_count += 1
                pbar.update(1)
                continue

            try:
                with open(vtt_file_path, "r", encoding="utf-8") as file:
                    contents = file.read()
            except Exception as e:
                context.log.error(f"Error reading file {vtt_file}: {e}")
                pbar.update(1)
                continue

            lines = contents.split("\n")
            current_timestamp = ""
            current_transcription = ""
            mixed_language_transcriptions = []
            arabic_only_transcriptions = []
            for line in lines:
                if "-->" in line:
                    # Save previous transcription if it contains Arabic characters
                    if arabic_pattern.search(current_transcription):
                        if english_pattern.search(current_transcription):
                            mixed_language_transcriptions.append((current_timestamp, current_transcription))
                        else:
                            arabic_only_transcriptions.append((current_timestamp, current_transcription))
                    # Reset for the next transcription
                    current_timestamp = line.strip()
                    current_transcription = ""
                else:
                    current_transcription += line.strip()
            # Process any remaining transcription after the loop ends
            if arabic_pattern.search(current_transcription):
                if english_pattern.search(current_transcription):
                    mixed_language_transcriptions.append((current_timestamp, current_transcription))
                else:
                    arabic_only_transcriptions.append((current_timestamp, current_transcription))

            delete_flag = False
            # Save mixed language transcriptions if any
            if mixed_language_transcriptions:
                try:
                    with open(mixedlanguage_file_path, "w", encoding="utf-8") as f:
                        for timestamp, transcription in mixed_language_transcriptions:
                            f.write("Timestamp: " + timestamp + "\n")
                            f.write("Transcription: " + transcription + "\n\n")
                    context.log.info(f"Processed file '{vtt_file}' - mixed language file saved.")
                    delete_flag = False
                except Exception as e:
                    context.log.error(f"Error writing mixed language file for {vtt_file}: {e}")
            else:
                context.log.info(f"Skipping file '{vtt_file}' - no mixed language transcriptions found.")
                delete_flag = True

            # Save Arabic-only transcriptions if any
            if arabic_only_transcriptions:
                try:
                    with open(arabic_only_file_path, "w", encoding="utf-8") as f:
                        for timestamp, transcription in arabic_only_transcriptions:
                            f.write("Timestamp: " + timestamp + "\n")
                            f.write("Transcription: " + transcription + "\n\n")
                    context.log.info(f"Processed file '{vtt_file}' - Arabic-only file saved.")
                    delete_flag = False
                except Exception as e:
                    context.log.error(f"Error writing Arabic-only file for {vtt_file}: {e}")
            else:
                context.log.info(f"Skipping file '{vtt_file}' - no Arabic-only transcriptions found.")
                delete_flag = True

            # If no valid transcriptions were found, delete the original VTT (and corresponding audio) file
            if delete_flag:
                try:
                    os.remove(vtt_file_path)
                    context.log.info(f"Deleted VTT file: {vtt_file}")
                    deleted_count += 1
                except Exception as e:
                    context.log.error(f"Error deleting VTT file {vtt_file}: {e}")
                try:
                    audio_file_name = vtt_file.replace(f".{lang}.vtt", ".mp3")
                    audio_file_path = os.path.join(audio_and_captions_folder, audio_file_name)
                    os.remove(audio_file_path)
                    context.log.info(f"Deleted audio file: {audio_file_name}")
                    deleted_count += 1
                except Exception as e:
                    context.log.error(f"Error deleting audio file {audio_file_name}: {e}")

            processed_count += 1
            pbar.update(1)

    context.log.info("Processing completed.")
    return {
        "status": "completed",
        "processed_files": processed_count,
        "deleted_files": deleted_count,
        "skipped_files": skipped_count,
    }

@asset(deps=[mixed_arabic_extractor])
def audio_segmenter(context: OpExecutionContext):
    """
    Asset to process MP3 files located in the audio-and-captions/Arabic/<dialect> folder.
    It searches for corresponding VTT files in the folders "arabic-only-<dialect>" and 
    "mixedlanguage-<dialect>", reads the timestamps and transcriptions from the VTT(s), and 
    splits the MP3 into segments accordingly. It then writes segment information into text files.
    """
    config = load_pipeline_config()
    dialect = config.get("dialect", "ECA")

    # Valid dialect options
    valid_options = ["ECA", "MSA"]

    if dialect is None:
        context.log.error("No dialect provided in configuration. Please supply one of: " + ", ".join(valid_options))
        return {"status": "failed", "message": "Missing dialect configuration"}
    if dialect not in valid_options:
        context.log.error(f"Invalid dialect: {dialect}. Valid options: " + ", ".join(valid_options))
        return {"status": "failed", "message": f"Invalid dialect: {dialect}"}

    # Define folder names based on dialect
    folder1 = f"arabic-only-{dialect}"
    folder2 = f"mixedlanguage-{dialect}"
    mp3_folder = os.path.join(os.getcwd(), f"audio-and-captions/Arabic/{dialect}")
    output_folder = os.path.join(os.getcwd(), f"output-folder-{dialect}")
    os.makedirs(output_folder, exist_ok=True)

    # Read all MP3 files from the mp3 folder
    mp3_files = [os.path.join(mp3_folder, file) for file in os.listdir(mp3_folder) if file.endswith(".mp3")]
    if not mp3_files:
        context.log.info("No MP3 files found in the specified folder.")
        return {"status": "failed", "message": "No MP3 files found"}

    # --- Helper functions ---
    def find_vtt_files(mp3_file, folder1=None, folder2=None):
        vtt_files = []
        if folder1 is None:
            # Search in folder2
            vtt_files_folder2 = [file for file in os.listdir(folder2) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
            vtt_files.extend(os.path.join(folder2, file) for file in vtt_files_folder2)
        elif folder2 is None:
            # Search in folder1
            vtt_files_folder1 = [file for file in os.listdir(folder1) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
            vtt_files.extend(os.path.join(folder1, file) for file in vtt_files_folder1)
        else:
            # Search in both folders
            vtt_files_folder1 = [file for file in os.listdir(folder1) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
            vtt_files.extend(os.path.join(folder1, file) for file in vtt_files_folder1)
            vtt_files_folder2 = [file for file in os.listdir(folder2) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
            vtt_files.extend(os.path.join(folder2, file) for file in vtt_files_folder2)
        return vtt_files

    def read_timestamps_and_transcriptions_from_vtt(vtt_file_1, vtt_file_2=None):
        timestamps = []
        transcriptions = []
        if vtt_file_1 and vtt_file_2:
            with open(vtt_file_1, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            with open(vtt_file_2, 'r', encoding='utf-8') as file:
                lines += file.readlines()
        else:
            with open(vtt_file_1, 'r', encoding='utf-8') as file:
                lines = file.readlines()

        for line in lines:
            if 'Timestamp' in line:
                timestamp_line = line.strip().split(' ')
                # Expecting a line such as: "Timestamp: <start_time> --> <end_time>"
                start_time = timestamp_line[1]
                end_time = timestamp_line[3]
                if '-->' in end_time:
                    end_time = timestamp_line[5]
                timestamps.append((start_time, end_time))
            if 'Transcription' in line:
                parts = line.strip().split('Transcription:')
                if len(parts) > 1:
                    transcriptions.append(parts[1].strip())
        return timestamps, transcriptions

    def timestamp_to_ms(timestamp):
        h, m, s = map(float, timestamp.split(':'))
        return int((h * 3600 + m * 60 + s) * 1000)

    def split_mp3(mp3_file, timestamps, transcriptions, output_folder):
        text_file = os.path.join(output_folder, 'text.txt')
        audio_paths_file = os.path.join(output_folder, 'audio_paths.txt')
        audio = AudioSegment.from_mp3(mp3_file)
        os.makedirs(output_folder, exist_ok=True)
        with open(audio_paths_file, 'a') as ap_file, open(text_file, 'a') as t_file:
            for i, (start_time, end_time) in enumerate(timestamps):
                start_ms = timestamp_to_ms(start_time)
                end_ms = timestamp_to_ms(end_time)
                segment = audio[start_ms:end_ms]
                output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1}.wav")
                segment.export(output_file, format="wav", parameters=["-ar", "16000", "-ac", "1"])
                t_file.write(f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1} {transcriptions[i]}\n")
                ap_file.write(f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1}.wav {os.path.join(os.getcwd(), output_file)}\n")

    def align_text_files(output_folder):
        audio_paths_file = os.path.join(output_folder, 'audio_paths.txt')
        text_file = os.path.join(output_folder, 'text.txt')
        if os.path.isfile(audio_paths_file):
            with open(audio_paths_file, 'r+') as file:
                existing_audio_paths = {line.strip() for line in file}
                file.seek(0)
                file.truncate()
                for line in natsorted(existing_audio_paths):
                    file.write(f"{line}\n")
        if os.path.isfile(text_file):
            with open(text_file, 'r+') as file:
                existing_text = {line.strip() for line in file}
                file.seek(0)
                file.truncate()
                for line in natsorted(existing_text):
                    file.write(f"{line}\n")

    # --- End Helper Functions ---

    total_processed = 0
    # Process each MP3 file with a progress bar
    for mp3_file in tqdm(mp3_files, desc="Processing MP3 files", unit="file"):
        vtt_files = find_vtt_files(mp3_file, folder1, folder2)
        if vtt_files:
            if len(vtt_files) < 2:
                context.log.info(f"Processing MP3 file: {mp3_file}")
                for vtt_file in vtt_files:
                    timestamps, transcriptions = read_timestamps_and_transcriptions_from_vtt(vtt_file)
                    split_mp3(mp3_file, timestamps, transcriptions, output_folder)
                    context.log.info(f"  MP3 file split based on timestamps in VTT file: {vtt_file}")
            else:
                context.log.info(f"Processing MP3 file: {mp3_file}")
                timestamps, transcriptions = read_timestamps_and_transcriptions_from_vtt(vtt_files[0], vtt_files[1])
                split_mp3(mp3_file, timestamps, transcriptions, output_folder)
                context.log.info(f"  MP3 file split based on timestamps in VTT files: {vtt_files[0]}, {vtt_files[1]}")
            total_processed += 1
        else:
            context.log.info(f"MP3 file: {mp3_file} has no corresponding VTT files.")

    align_text_files(output_folder)
    context.log.info("Audio segmentation completed.")
    return {"status": "completed", "processed_files": total_processed}

def check_lang_captions(video_url, lang):
    command = [
        'yt-dlp',
        '--list-subs',
        '--skip-download',
        '--write-auto-sub',
        '--sub-lang',
        lang,
        video_url
    ]
    try:
        output = subprocess.check_output(command, universal_newlines=True)
        if lang in output.lower():
            return True
        return False
    except subprocess.CalledProcessError:
        return False

def get_video_id(url):
    video_id = re.findall(r'(?:v=|youtu\.be/)([^&\n]+)', url)
    if video_id:
        return video_id[0]
    return None

def check_file_existence(video_id, lang):
    audio_path = os.path.join("audio-and-captions", f"{video_id}.mp3")
    sub_path = os.path.join("audio-and-captions", f"{video_id}.{lang}.vtt")
    return os.path.exists(audio_path) and os.path.exists(sub_path)

def download_lang_captions(video_url, lang):
    command = [
        'yt-dlp',
        '--write-sub',
        '--sub-lang',
        lang,
        '--extract-audio',
        '--audio-format',
        'mp3',
        '--output',
        'audio-and-captions/%(id)s.%(ext)s',
        video_url
    ]
    try:
        subprocess.check_output(command, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(f"Error downloading captions for {video_url}: {e}")

# ---------------- Jobs and Sensor ----------------
@job
def process_and_download_job():
    optimized_youtube_keyword_processor()
    filter_song_urls()
    download_audio_and_captions()
    language_detection_client()
    dialect_detection_client()
    mixed_arabic_extractor()
    audio_segmenter()

@sensor(
    job=process_and_download_job,
    minimum_interval_seconds=5,
    default_status=DefaultSensorStatus.RUNNING,
)
def keyword_file_sensor(context):
    keywords = read_keywords()
    states = load_search_states(STATE_FILE_PATH)
    new_keywords = [kw for kw in keywords if kw not in states]
    if new_keywords:
        context.log.info(f"New keywords detected: {new_keywords}")
        run_key = "new_" + "_".join(sorted(new_keywords))
        return RunRequest(run_key=run_key)
    return None

defs = Definitions(
    assets=[optimized_youtube_keyword_processor, filter_song_urls, download_audio_and_captions, language_detection_client, dialect_detection_client, mixed_arabic_extractor, audio_segmenter],
    jobs=[process_and_download_job],
    sensors=[keyword_file_sensor],
)
