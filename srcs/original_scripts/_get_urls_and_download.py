import os
import subprocess
import re
from tqdm import tqdm

def check_arabic_captions(video_url, lang):
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
            # print("Arabic closed captions are available.")
            return True
        else:
            # print("Arabic closed captions are not available.")
            return False
    except subprocess.CalledProcessError as e:
        print("Video is private or broken!")

def get_video_id(url):
    video_id = re.findall(r'youtu\.be/([^/]+)', url)
    if video_id:
        return video_id[0]
    else:
        return None

def check_file_existence(video_id, lang):
    if os.path.exists(os.path.join("audio-and-captions", f"{video_id}.mp3")) and os.path.exists(os.path.join("audio-and-captions", f"{video_id}.ar.vtt")):
        return True
    else:
        return False

def download_arabic_captions(video_url):
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
        print("Arabic captions downloaded successfully.")
    except subprocess.CalledProcessError as e:
        print("An error occurred:", e)


folder_path = os.path.join(os.getcwd(), f"url_list")  # Path to the folder containing urls
file_list = [f for f in os.listdir(folder_path) if f.endswith(".txt")]

lang = 'ar' # Target language
subfolder = "Arabic" # Target language subfolder

# Iterate over each url text file
for file_name in file_list:
    file_path = os.path.join(folder_path, file_name)
    print("Processing file:", file_path)

    with open(file_path, "r") as file:
        urls = file.readlines()

    # Iterate over each URL in the file
    count_urls_with_cc = 0
    for url in tqdm(urls, desc="Processing URLs"):
        url = url.strip()  # Remove leading/trailing whitespace and newline characters

        result_check_cc = check_arabic_captions(url, lang)
        if result_check_cc:
          print(f'{url} has Target Subtitles')
          # Download captions
          video_id = get_video_id(url)
          if check_file_existence(video_id, lang) == False:
            download_arabic_captions(url)
          else:
            print(f'Skipping: already downloaded {video_id}')
          count_urls_with_cc +=1
        # print("Processing URL:", url)

    print("Finished processing file:", file_path)
    print(f'Number of videos that has target subtitles: {count_urls_with_cc}/{len(urls)-count_urls_with_cc}')
