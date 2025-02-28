import os
import sys
from mutagen.mp3 import MP3

def calculate_total_duration(directory):
    total_duration = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.mp3'):
                try:
                    audio = MP3(os.path.join(root, file))
                    total_duration += audio.info.length
                except Exception as e:
                    print(f"Error processing file {file}: {e}")
    return total_duration

if len(sys.argv) != 2:
    print("Usage: python script.py <folder_path>")
    sys.exit(1)

folder_path = sys.argv[1]
total_duration_seconds = calculate_total_duration(folder_path)
hours = total_duration_seconds // 3600
minutes = (total_duration_seconds % 3600) // 60
seconds = total_duration_seconds % 60
print(f"Total duration: {hours} hours, {minutes} minutes, {seconds} seconds")
