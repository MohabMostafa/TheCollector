import os
import re
from tqdm import tqdm
import sys

options = ["ECA", "MSA"]
def print_options(options):
    for option in options:
        print(f"- {option}")

if len(sys.argv) == 2:
    dialect = sys.argv[1]
    print(f"Received argument: {dialect}")
    if dialect in options:
        print(f"Received valid argument: {dialect}")
    else:
        print(f"Invalid argument: {dialect}. Please choose from the available options below.")
        print_options(options)
        sys.exit()
else:
    print("Usage: python script_name.py <argument>")
    print("Example: python _mixed_arabic_extractor.py ECA")
    print("\nAvailable options:")
    print_options(options)
    sys.exit()

lang = "ar"

# Define the regular expression patterns for Arabic and English characters
arabic_pattern = re.compile(r'[\u0600-\u06FF]+')
english_pattern = re.compile(r'[a-zA-Z]+')

# Create the "mixedlanguage" and "arabic-only" folders if they don't exist
mixedlanguage_folder = os.path.join(os.getcwd(), f"mixedlanguage-{dialect}")
arabic_only_folder = os.path.join(os.getcwd(), f"arabic-only-{dialect}")
for folder in [mixedlanguage_folder, arabic_only_folder]:
    os.makedirs(folder, exist_ok=True)

# Scan the "audio_and_captions" folder
audio_and_captions_folder = os.path.join(os.getcwd(), f"audio-and-captions/Arabic/{dialect}")
vtt_files = [file for file in os.listdir(audio_and_captions_folder) if file.endswith(".vtt")]

# Process each .txt file with a progress bar
with tqdm(total=len(vtt_files), desc="Processing files", unit="file") as pbar:
    for vtt_file in vtt_files:
        # Define the paths for the current file
        vtt_file_path = os.path.join(audio_and_captions_folder, vtt_file)
        mixedlanguage_file_path = os.path.join(mixedlanguage_folder, vtt_file[:-4] + "_mixedlanguage.vtt")
        arabic_only_file_path = os.path.join(arabic_only_folder, vtt_file[:-4] + "_arabic_only.vtt")

        # Skip processing if the mixedlanguage and arabic-only files already exist
        if os.path.exists(mixedlanguage_file_path) and os.path.exists(arabic_only_file_path):
            tqdm.write(f"Skipping file '{vtt_file}' - mixedlanguage and arabic-only files already exist.")
            pbar.update(1)
            continue

        # Read the contents of the text file
        with open(vtt_file_path, 'r', encoding='utf-8') as file:
            contents = file.read()

        # Split the contents into lines
        lines = contents.split('\n')

        # Initialize variables to store the current timestamp and transcription
        current_timestamp = ''
        current_transcription = ''

        # Iterate over the lines and extract the mixed language and Arabic-only transcriptions
        mixed_language_transcriptions = []
        arabic_only_transcriptions = []
        for line in lines:
            if '-->' in line:
                # Store the previous transcription if it contains mixed languages or is Arabic-only
                if arabic_pattern.search(current_transcription):
                    if english_pattern.search(current_transcription):
                        mixed_language_transcriptions.append((current_timestamp, current_transcription))
                    else:
                        arabic_only_transcriptions.append((current_timestamp, current_transcription))

                # Reset the variables for the new transcription
                current_timestamp = line.strip()
                current_transcription = ''
            else:
                current_transcription += line.strip()

        # Check the last transcription after the loop ends
        if arabic_pattern.search(current_transcription):
            if english_pattern.search(current_transcription):
                mixed_language_transcriptions.append((current_timestamp, current_transcription))
            else:
                arabic_only_transcriptions.append((current_timestamp, current_transcription))

        delete_flag = False
        # Save the extracted mixed language transcriptions with timestamps to the mixedlanguage file
        if mixed_language_transcriptions:
            with open(mixedlanguage_file_path, 'w', encoding='utf-8') as mixedlanguage_file:
                for timestamp, transcription in mixed_language_transcriptions:
                    mixedlanguage_file.write("Timestamp: " + timestamp + "\n")
                    mixedlanguage_file.write("Transcription: " + transcription + "\n\n")
            tqdm.write(f"Processed file '{vtt_file}' - mixedlanguage file saved.")
            delete_flag = False
        else:
            tqdm.write(f"Skipping file '{vtt_file}' - no mixed language transcriptions found.")
            delete_flag = True

        # Save the extracted Arabic-only transcriptions with timestamps to the arabic-only file
        if arabic_only_transcriptions:
            with open(arabic_only_file_path, 'w', encoding='utf-8') as arabic_only_file:
                for timestamp, transcription in arabic_only_transcriptions:
                    arabic_only_file.write("Timestamp: " + timestamp + "\n")
                    arabic_only_file.write("Transcription: " + transcription + "\n\n")
            tqdm.write(f"Processed file '{vtt_file}' - arabic-only file saved.")
            delete_flag = False
        else:
            tqdm.write(f"Skipping file '{vtt_file}' - no Arabic-only transcriptions found.")
            delete_flag = True

        if delete_flag == True:
          try:
            os.remove(f'audio-and-captions/Arabic/{dialect}/{vtt_file}')
            print("Deleted: ", vtt_file)
          except:
              print("VTT file not found.")
              continue
          
          try:
            os.remove(f'audio-and-captions/Arabic/{dialect}/{vtt_file.replace(f".{lang}.vtt", ".mp3")}')
            print("Deleted: ", vtt_file.replace(f".{lang}.vtt", ".mp3"))
          except:
              print("Audio file not found.")
              continue

        pbar.update(1)

tqdm.write("Processing completed.")

