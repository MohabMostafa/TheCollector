import os
import random
import shutil
from transformers import pipeline

# Define the folder path containing the VTT files
folder_path = 'audio-and-captions/Arabic'

# Define the model name
model_name = "AMR-KELEG/ADI-NADI-2023"

# Load the pipeline for text classification
classifier = pipeline("text-classification", model=model_name)

def classify_dialect(text):
    # Classify the dialect of the given text
    results = classifier(text)
    return results[0]['label'], results[0]['score']

def process_vtt_file(file_path):
    # Read the contents of the VTT file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Extract non-empty dialogue lines
    dialogues = [line.strip() for line in lines if line.strip() and '-->' not in line]

    # Randomly select 25 dialogues for analysis, if there are fewer, select all
    selected_dialogues = random.sample(dialogues, min(50, len(dialogues)))

    # Initialize a dictionary to count the frequency of each detected dialect
    dialect_frequency = {}

    # Classify the dialect for each selected dialogue
    for dialogue in selected_dialogues:
        predicted_class, _ = classify_dialect(dialogue)
        if predicted_class not in dialect_frequency:
            dialect_frequency[predicted_class] = 0
        dialect_frequency[predicted_class] += 1

    # Determine the majority dialect by comparing frequencies
    majority_dialect = max(dialect_frequency, key=dialect_frequency.get)

    # Check if the "Egypt" dialect has the majority
    target_sub_folder = 'ECA' if majority_dialect == 'Egypt' else 'MSA'
    target_folder_path = os.path.join(os.path.dirname(file_path), target_sub_folder)

    # Create the sub-folder if it doesn't exist
    if not os.path.exists(target_folder_path):
        os.makedirs(target_folder_path)

    # Move the file
    shutil.move(file_path, os.path.join(target_folder_path, os.path.basename(file_path)))

    # Construct the audio file path based on the VTT file name
    audio_file_path = file_path.replace('.ar.vtt', '.mp3')

    # Move the audio file if it exists
    if os.path.exists(audio_file_path):
        shutil.move(audio_file_path, os.path.join(target_folder_path, os.path.basename(audio_file_path)))

    print(f"Moved '{os.path.basename(file_path)}' and corresponding audio file to '{target_folder_path}' based on majority dialect: {majority_dialect}")
    # print(f"Moved '{file_path}' to '{target_folder_path}' based on majority dialect: {majority_dialect}")

# Loop over all VTT files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.vtt'):
        file_path = os.path.join(folder_path, file_name)
        process_vtt_file(file_path)
