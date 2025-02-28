import os
from pydub import AudioSegment
import logging
from natsort import natsorted
import sys

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_audio_paths_file(output_folder):
    audio_paths_file = os.path.join(output_folder, 'audio_paths.txt')

    existing_paths = set()

    # Check if the audio_paths_file already exists
    if os.path.isfile(audio_paths_file):
        with open(audio_paths_file, 'r') as file:
            existing_paths = {line.strip() for line in file}

    with open(audio_paths_file, 'a') as file:
        for root, _, files in os.walk(output_folder):
            # files = natsorted(files)
            for filename in files:
                if filename.endswith(".wav"):
                    file_path = os.path.join(root, filename)

                    # Check if the file_path is not already in the audio_paths_file
                    if file_path not in existing_paths:
                        file.write(f"{filename[:-4]} {file_path}\n")
                        existing_paths.add(file_path)

    logging.info(f"Audio paths file updated: {audio_paths_file}")

def read_mp3_files(folder_path):
    mp3_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".mp3")]
    return mp3_files

def find_vtt_files(mp3_file, folder1=None, folder2=None):
    vtt_files = []

    if folder1 == None:
        # Search for VTT files in the second folder
        vtt_files_folder2 = [file for file in os.listdir(folder2) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
        vtt_files.extend(os.path.join(folder2, file) for file in vtt_files_folder2)
    elif folder2 == None:
        # Search for VTT files in the first folder
        vtt_files_folder1 = [file for file in os.listdir(folder1) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
        vtt_files.extend(os.path.join(folder1, file) for file in vtt_files_folder1)
    else:
        # Search for VTT files in the first folder
        vtt_files_folder1 = [file for file in os.listdir(folder1) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
        vtt_files.extend(os.path.join(folder1, file) for file in vtt_files_folder1)

        # Search for VTT files in the second folder
        vtt_files_folder2 = [file for file in os.listdir(folder2) if file.startswith(os.path.basename(mp3_file[:-4])) and file.endswith(".vtt")]
        vtt_files.extend(os.path.join(folder2, file) for file in vtt_files_folder2)

    return vtt_files

def read_timestamps_and_transcriptions_from_vtt(vtt_file_1, vtt_file_2 = None):
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
            start_time = timestamp_line[1]
            end_time = timestamp_line[3]

            # Ensure the timestamps are in the correct format
            if '-->' in end_time:
                end_time = timestamp_line[5]

            timestamps.append((start_time, end_time))

        if 'Transcription' in line:
            transcriptions.append(line.strip().split('Transcription:')[1])

    return timestamps, transcriptions

def split_mp3(mp3_file, timestamps, transcriptions, output_folder):
    text_file = os.path.join(output_folder, 'text')
    audio_paths_file = os.path.join(output_folder, 'audio_paths')
    audio = AudioSegment.from_mp3(mp3_file)

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    with open(audio_paths_file, 'a') as ap_file:
        with open(text_file, 'a') as t_file:
            for i, (start_time, end_time) in enumerate(timestamps):
                start_ms = timestamp_to_ms(start_time)
                end_ms = timestamp_to_ms(end_time)
                segment = audio[start_ms:end_ms]

                # Save the segment to the output folder with a unique name in WAV format
                output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1}.wav")
                segment.export(output_file, format="wav", parameters=["-ar", "16000", "-ac", "1"])

                # Save segment with transcriptions in text.txt file
                t_file.write(f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1} {transcriptions[i]}\n")

                # Save segment name and it's path in audio_paths.txt file
                ap_file.write(f"{os.path.splitext(os.path.basename(mp3_file))[0]}_segment_{i+1}.wav {os.path.join(os.getcwd(), output_file)}\n")

def timestamp_to_ms(timestamp):
    h, m, s = map(float, timestamp.split(':'))
    return int((h * 3600 + m * 60 + s) * 1000)

def align_text_files(output_folder):
    # Get text files location
    audio_paths_file = os.path.join(output_folder, 'audio_paths.txt')
    text_file = os.path.join(output_folder, 'text.txt')

    # Read text files content, sort it and overwrite the file
    with open(audio_paths_file, 'r+') as file:
        existing_audio_paths = {line.strip() for line in file}
        file.seek(0)
        file.truncate()
        for line in natsorted(existing_audio_paths):
            file.write(f"{line}\n")
        file.close()

    with open(text_file, 'r+') as file:
        existing_text = {line.strip() for line in file}
        file.seek(0)
        file.truncate()
        for line in natsorted(existing_text):
            file.write(f"{line}\n")
        file.close()

def print_options(options):
        for option in options:
            print(f"- {option}")

def main():
    setup_logging()

    options = ["ECA", "MSA"]

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
        print("Example: python _prepare_data.py ECA")
        print("\nAvailable options:")
        print_options(options)
        sys.exit()
        
    # folders containing VTT files
    folder1 = f'arabic-only-{dialect}'
    folder2 = f'mixedlanguage-{dialect}'

    mp3_folder = f'audio-and-captions/Arabic/{dialect}'
    output_folder = f'output-folder-{dialect}'

    mp3_files = read_mp3_files(mp3_folder)

    if not mp3_files:
        print("No MP3 files found in the specified folder.")
        return

    for mp3_file in mp3_files:
        vtt_files = find_vtt_files(mp3_file, folder1, folder2)

        if vtt_files:
            if len(vtt_files) < 2:
                print(f"\nProcessing MP3 File: {mp3_file}")
                for vtt_file in vtt_files:
                    timestamps, transcriptions = read_timestamps_and_transcriptions_from_vtt(vtt_file)
                    split_mp3(mp3_file, timestamps, transcriptions, output_folder)
                    print(f"  MP3 File has been split based on timestamps in VTT file: {vtt_file}")
            else:
                print(f"\nProcessing MP3 File: {mp3_file}")
                timestamps, transcriptions = read_timestamps_and_transcriptions_from_vtt(vtt_files[0], vtt_files[1])
                split_mp3(mp3_file, timestamps, transcriptions, output_folder)
                print(f"  MP3 File has been split based on timestamps in VTT files: {vtt_files[0]}, {vtt_files[1]}")
        else:
            print(f"\nMP3 File: {mp3_file}")
            print("  No corresponding VTT files found.")

if __name__ == "__main__":
    main()
