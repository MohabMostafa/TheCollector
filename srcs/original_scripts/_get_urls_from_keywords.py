import yt_dlp
import os
import random

def process_keywords_in_chunks(keywords, file_path, existing_urls, max_results, lang, proxies):
    total_duration = 0

    for i in range(0, len(keywords), 10):
        # Get the next 10 keywords, or all remaining if less than 10
        chunk = keywords[i:i + 10]

        # Call the function with the current chunk of keywords
        video_captions_urls, chunk_duration = search_and_check_captions(chunk, max_results, lang, proxies)
        total_duration += chunk_duration

        # Write the results to the file
        with open(file_path, 'a') as f:
            for url in video_captions_urls:
                if url not in existing_urls:
                    f.write(url + '\n')

    print(f"Total duration of the saved URLs: {total_duration} seconds")


def search_and_check_captions(keywords, max_results, lang, proxies):
    video_captions_urls = []
    total_duration = 0
    for i, keyword in enumerate(keywords):

        proxy = random.choice(proxies)
        print(f"\n***********\nProcessing keyword {i+1}/{len(keywords)}: {keyword}\n***********\n")

        ydl_opts = {
        'skip_download': True,
        'subtitleslangs': [lang],
        'outtmpl': '%(id)s.%(ext)s',
        'geo_verification_proxy': proxy,
        'dateafter': 20200101, # YYYYMMDD
        }

        search_url = f"ytsearch{max_results}:{keyword}"
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info_dict = ydl.extract_info(search_url, download=False)
            except:
                continue

        print(f"\n***********\nGot {len(info_dict['entries'])} results for {keyword}\n***********\n")

        for entry in info_dict['entries']:
            url = entry['original_url']
            try:
                info = ydl.extract_info(url, download=False)
            except:
                continue

            # Check if video is a Music video
            music_check = info.get('categories')
            if music_check != None:
                if music_check and 'Music' in music_check or 'Entertainment' in music_check:
                    continue

            # Skip videos shorter than a minute (or with unknown duration)
            duration = info.get('duration')
            if duration != None:
                if duration and duration < 60:
                    continue

            if entry['subtitles']:
                if lang in entry['subtitles']:
                    video_captions_urls.append(url)
                    total_duration += duration
                
    return video_captions_urls, total_duration

# Read the keywords from the "keywords.txt" file
with open('keywords/keywords.txt', 'r') as f:
    keywords = f.read().splitlines()
with open('proxies.txt', 'r') as f:
    proxies = f.read().splitlines()

max_results = 10 # Set max number of resutls
lang = 'ar' # Set target language
print(f'Target language: {lang}')

# Create the "url_list" directory if it doesn't exist
if not os.path.exists('url_list'):
    os.makedirs('url_list')

# Check if the file exists
file_path = 'url_list/urls.txt'
if os.path.exists(file_path):
    with open(file_path, 'r') as f:
        existing_urls = f.read().splitlines()
else:
    existing_urls = []

process_keywords_in_chunks(keywords, file_path, existing_urls, max_results, lang, proxies)
