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

@asset
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

# ---------------- Jobs and Sensor ----------------
@job
def process_and_download_job():
    optimized_youtube_keyword_processor()
    filter_song_urls()
    download_audio_and_captions()

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
    assets=[optimized_youtube_keyword_processor, filter_song_urls, download_audio_and_captions],
    jobs=[process_and_download_job],
    sensors=[keyword_file_sensor],
)
