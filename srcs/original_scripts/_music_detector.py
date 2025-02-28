import json
import subprocess
from tqdm import tqdm

def is_song(url):
    try:
        # Use yt-dlp to get video info as JSON
        result = subprocess.run(["yt-dlp", "-j", url], capture_output=True, text=True)
        video_info = json.loads(result.stdout)

        # Criteria for determining if a video is a song
        categories_to_check = ["music", "entertainment"]
        keywords_to_check = ["official music video", "lyric video", "audio"]

        # Check if the category contains "music" or "entertainment"
        if "categories" in video_info:
            if any(category.lower() in categories_to_check for category in video_info["categories"]):
                return True

        # Check if tags contain "music"
        if "tags" in video_info:
            if any("music" in tag.lower() for tag in video_info["tags"]):
                return True

        # Check title and description for specific keywords
        title_description = video_info.get("title", "").lower() + " " + video_info.get("description", "").lower()
        if any(keyword in title_description for keyword in keywords_to_check):
            return True
    except Exception as e:
        print(f"Error processing {url}: {e}")
    return False

def filter_songs(file_path):
    # List to hold URLs that are not songs
    not_songs = []
    
    with open(file_path, 'r') as file:
        urls = file.readlines()
    
    # Use tqdm to show progress
    for url in tqdm(urls, desc="Processing URLs"):
        url = url.strip()
        if not is_song(url):
            not_songs.append(url)
        else:
            print(f"\nRemoving {url} as it is likely a song.")  # New line for clarity in tqdm output

    # Rewrite the file without the songs
    with open(file_path, 'w') as file:
        for url in not_songs:
            file.write(url + "\n")

file_path = 'url_list/urls.txt'
filter_songs(file_path)
