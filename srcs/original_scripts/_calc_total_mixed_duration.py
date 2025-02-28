import os
import re

# Define the regular expression pattern for timestamps
timestamp_pattern = re.compile(r'(\d+):(\d+):(\d+)\.(\d+) --> (\d+):(\d+):(\d+)\.(\d+)')

# Create a variable to store the total duration in seconds
total_duration = 0

# Path to the "mixedlanguage" folder
mixedlanguage_folder = os.path.join(os.getcwd(), "mixedlanguage")

# Iterate over each .vtt file in the folder
for file in os.listdir(mixedlanguage_folder):
    if file.endswith(".vtt"):
        file_path = os.path.join(mixedlanguage_folder, file)

        # Read the contents of the .vtt file
        with open(file_path, 'r', encoding='utf-8') as vtt_file:
            contents = vtt_file.read()

        # Find all timestamps in the file
        timestamps = re.findall(timestamp_pattern, contents)

        # Calculate the duration for each timestamp and sum them up
        for timestamp in timestamps:
            start_hour, start_min, start_sec, start_ms, end_hour, end_min, end_sec, end_ms = map(int, timestamp)
            start_time = start_hour * 3600 + start_min * 60 + start_sec + start_ms / 1000
            end_time = end_hour * 3600 + end_min * 60 + end_sec + end_ms / 1000
            duration = end_time - start_time
            total_duration += duration

# Convert the total duration to hours, minutes, and seconds
total_duration = int(total_duration)
hours = total_duration // 3600
minutes = (total_duration % 3600) // 60
seconds = total_duration % 60

# Display the total duration in time format
print("Total duration of utterances:", hours, "hours,", minutes, "minutes, and", seconds, "seconds")
