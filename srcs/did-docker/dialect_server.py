from flask import Flask, jsonify, request
import os
import random
import shutil
from transformers import pipeline
import threading
import time

app = Flask(__name__)

# Global variables
classifier = None
model_initialized = False
is_processing = False
processing_results = None

MODEL_NAME = "AMR-KELEG/ADI-NADI-2023"
FOLDER_PATH = "audio-and-captions/Arabic"

def initialize_model():
    global classifier, model_initialized
    try:
        classifier = pipeline("text-classification", model=MODEL_NAME)
        model_initialized = True
        print("Dialect model loaded successfully!")
    except Exception as e:
        print(f"Error loading dialect model: {e}")
        model_initialized = False

# Start model initialization in a separate thread
init_thread = threading.Thread(target=initialize_model)
init_thread.daemon = True
init_thread.start()

def classify_dialect(text):
    results = classifier(text)
    return results[0]['label'], results[0]['score']

def process_vtt_file(file_path):
    # Read the VTT file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Extract non-empty dialogue lines (ignoring timestamp lines)
    dialogues = [line.strip() for line in lines if line.strip() and '-->' not in line]

    # Randomly select up to 50 dialogues for analysis (or all if fewer)
    selected_dialogues = random.sample(dialogues, min(50, len(dialogues)))

    # Count the frequency of each detected dialect
    dialect_frequency = {}
    for dialogue in selected_dialogues:
        predicted_class, _ = classify_dialect(dialogue)
        dialect_frequency[predicted_class] = dialect_frequency.get(predicted_class, 0) + 1

    # Determine the majority dialect
    majority_dialect = max(dialect_frequency, key=dialect_frequency.get)

    # Decide target sub-folder: 'ECA' if majority is 'Egypt', else 'MSA'
    target_sub_folder = 'ECA' if majority_dialect == 'Egypt' else 'MSA'
    target_folder_path = os.path.join(os.path.dirname(file_path), target_sub_folder)

    # Create the sub-folder if it doesn't exist
    if not os.path.exists(target_folder_path):
        os.makedirs(target_folder_path)

    # Move the VTT file
    shutil.move(file_path, os.path.join(target_folder_path, os.path.basename(file_path)))

    # Construct the corresponding audio file path and move it if it exists
    audio_file_path = file_path.replace('.ar.vtt', '.mp3')
    if os.path.exists(audio_file_path):
        shutil.move(audio_file_path, os.path.join(target_folder_path, os.path.basename(audio_file_path)))

    print(f"Moved '{os.path.basename(file_path)}' and corresponding audio file to '{target_folder_path}' based on majority dialect: {majority_dialect}")
    return {
        "file": os.path.basename(file_path),
        "majority_dialect": majority_dialect,
        "target_folder": target_sub_folder
    }

def process_all_vtt_files():
    results = []
    if not os.path.exists(FOLDER_PATH):
        return {"status": "error", "message": f"Folder '{FOLDER_PATH}' does not exist"}
    for file_name in os.listdir(FOLDER_PATH):
        if file_name.endswith('.vtt'):
            file_path = os.path.join(FOLDER_PATH, file_name)
            try:
                result = process_vtt_file(file_path)
                results.append(result)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                results.append({"file": file_name, "status": "error", "message": str(e)})
    return {"status": "completed", "results": results}

@app.route('/health', methods=['GET'])
def health_check():
    if not model_initialized:
        return jsonify({"status": "initializing", "message": "Dialect model is still initializing"})
    return jsonify({"status": "healthy", "model_loaded": True})

@app.route('/process', methods=['POST'])
def process_dialect():
    global is_processing, processing_results
    if not model_initialized:
        return jsonify({"status": "error", "message": "Dialect model is still initializing, please try again later"})
    if is_processing:
        return jsonify({"status": "processing", "message": "Dialect processing is already in progress"})
    
    def process_thread():
        global is_processing, processing_results
        try:
            processing_results = process_all_vtt_files()
            is_processing = False
        except Exception as e:
            processing_results = {"status": "error", "message": str(e)}
            is_processing = False

    is_processing = True
    thread = threading.Thread(target=process_thread)
    thread.start()
    
    return jsonify({"status": "started", "message": "Dialect processing has started"})

@app.route('/status', methods=['GET'])
def status():
    if is_processing:
        return jsonify({"status": "processing", "message": "Dialect processing is in progress"})
    elif processing_results is not None:
        return jsonify(processing_results)
    else:
        return jsonify({"status": "idle", "message": "No processing has been initiated"})

if __name__ == '__main__':
    print("Starting dialect detection server on port 3003")
    app.run(host='0.0.0.0', port=3003, debug=False)
