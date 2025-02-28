import yt_dlp
import os
import random
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm

def save_progress(file_path, json_path, existing_urls, videos_info, total_duration):
    with open(file_path, 'w') as f:
        f.writelines(f"{url}\n" for url in existing_urls)
    
    with open(json_path, 'w') as f:
        json.dump({'videos': videos_info, 'total_duration': total_duration, 'videos_count': len(videos_info)}, f, ensure_ascii=False, indent=4)

def contains_arabic(text):
    arabic_range = re.compile('[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+')
    return bool(arabic_range.search(text))

def has_arabic_subtitles(entry):
    subtitles = entry.get('subtitles', {})
    return 'ar' in subtitles or 'arabic' in subtitles

def load_search_states(state_file_path):
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r') as f:
            return json.load(f)
    return {}

def save_search_states(state_file_path, states):
    with open(state_file_path, 'w') as f:
        json.dump(states, f, ensure_ascii=False, indent=4)

def process_keyword(keyword, proxies, ydl_opts, existing_urls, lock, states, state_file_path):
    new_videos_info = []
    total_duration = 0
    ydl_opts['geo_verification_proxy'] = random.choice(proxies)
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            result = ydl.extract_info(f"ytsearch999:{keyword}", download=False)
            for entry in result.get('entries', []):
                url = entry.get('webpage_url')
                if not url or url in existing_urls or not has_arabic_subtitles(entry):
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

def process_keywords_and_update_json(keywords, file_path, json_path, proxies, state_file_path):
    if not os.path.exists('url_list'):
        os.makedirs('url_list')
    
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
        'dateafter': 20200101,
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for keyword in keywords:
            if states.get(keyword) != "done":
                states[keyword] = "in progress"
                futures[executor.submit(process_keyword, keyword, proxies, ydl_opts, existing_urls, lock, states, state_file_path)] = keyword
        
        save_search_states(state_file_path, states)  # Save the initial state
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Keywords"):
            keyword = futures[future]
            try:
                new_videos, duration = future.result()
                videos_info.extend(new_videos)
                total_duration += duration
                save_progress(file_path, json_path, existing_urls, videos_info, total_duration)
            except Exception as exc:
                print(f"{keyword} generated an exception: {exc}")

    print(f"Updated total duration: {total_duration} seconds")

# Setup and function call
keywords_path = 'keywords/keywords.txt'
proxies_path = 'proxies.txt'
file_path = 'url_list/urls.txt'
json_path = 'url_list/videos_info.json'
state_file_path = 'url_list/search_states.json'  # New file path for search states

if not os.path.exists(keywords_path) or not os.path.exists(proxies_path):
    print("Keywords or proxies files are missing.")
else:
    with open(keywords_path, 'r') as f:
        keywords = f.read().splitlines()

    with open(proxies_path, 'r') as f:
        proxies = f.read().splitlines()

    process_keywords_and_update_json(keywords, file_path, json_path, proxies, state_file_path)
