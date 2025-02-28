import os
import re
from datetime import datetime, timedelta

# Step 1: Load the dictionary file
dictionary_path = 'arabic_english_similar_words.txt'
word_map = {}
with open(dictionary_path, 'r', encoding='utf-8') as dict_file:
    for line in dict_file:
        english, arabic = line.strip().split(': ')
        word_map[arabic.strip()] = english.strip()

# Step 2: Define the folder containing the VTT files
folder_path = 'audio-and-captions/Arabic/ECA'

# Initialize counters
total_segments = 0
updated_segments = 0
total_duration = timedelta()

# Function to calculate duration from timestamps
def calculate_duration(start, end):
    start_time = datetime.strptime(start, '%H:%M:%S.%f')
    end_time = datetime.strptime(end, '%H:%M:%S.%f')
    return end_time - start_time

# Step 3: Iterate over each VTT file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.vtt'):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.readlines()
        
        new_content = []
        process_next_line = False
        for line in content:
            # Check for timestamp lines and process them
            match = re.match(r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})', line)
            if match:
                process_next_line = True
                start_timestamp, end_timestamp = match.groups()
                new_content.append(line)
                continue
            
            if process_next_line:
                total_segments += 1
                transcription = line.strip()
                segment_updated = False
                # Check and replace all occurrences of each word in the dictionary
                for arabic, english in word_map.items():
                    pattern = r'\b' + re.escape(arabic) + r'\b'
                    if re.search(pattern, transcription):
                        print(f'Before: {transcription}')
                        transcription = re.sub(pattern, english, transcription)
                        print(f'after: {transcription}')
                        segment_updated = True
                
                if segment_updated:
                    updated_segments += 1
                    duration = calculate_duration(start_timestamp, end_timestamp)
                    total_duration += duration
                    # Optionally print before and after for each updated line
                    # print(f'Updated line: {transcription}')
                new_content.append(transcription + '\n')  # Add newline for formatting
                process_next_line = False
            else:
                new_content.append(line)

        # Save the modified content (uncomment in actual use)
        # with open(file_path, 'w', encoding='utf-8') as file:
        #     file.writelines(new_content)

print(f'Total segments processed: {total_segments}')
print(f'Total segments updated: {updated_segments}')
print(f'Total duration of updated segments: {total_duration}')
