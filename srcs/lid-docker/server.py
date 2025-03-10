from flask import Flask, jsonify, request
import os
import threading
from speechbrain.inference import EncoderClassifier
import torch
import time
from natsort import natsorted, ns
import shutil
import glob
import torchaudio
from tqdm import tqdm
from collections import Counter

app = Flask(__name__)

# Global variables
is_processing = False
processing_results = None
language_classifier = None

def most_frequent(List):
    occurence_count = Counter(List)
    most_frequent_lang = occurence_count.most_common(1)[0][0]
    return most_frequent_lang

def preprocess(signal: torch.tensor, sr: int) -> torch.tensor:
    CHANNELS = 1
    SAMPLE_RATE = 16_000

    # Resample the audio (if not already)
    if sr != SAMPLE_RATE:
        resample_transform = torchaudio.transforms.Resample(
            orig_freq=sr, new_freq=SAMPLE_RATE
        )
        resampled_waveform = resample_transform(signal)
    else:
        resampled_waveform = signal

    # Convert to monochannel (if not already)
    if resampled_waveform.shape[0] != CHANNELS:
        monochannel_waveform = torch.mean(resampled_waveform, dim=0, keepdim=True)
    else:
        monochannel_waveform = resampled_waveform

    return monochannel_waveform

def detect_lang(path: str, classifier: EncoderClassifier) -> str:
    signal, sr = torchaudio.load(path, num_frames=10000000)
    signal = preprocess(signal=signal, sr=sr)
    signal = signal.squeeze(0)

    total_samples = len(signal)
    WINDOW_SIZE = 30
    STRIDE = 30
    window_size_samples = int(16_000 * WINDOW_SIZE)
    stride_size_samples = int(16_000 * STRIDE)
    preds = []

    # If audio file is less than or equal WINDOW_SIZE seconds
    if total_samples <= window_size_samples:
        start = 0
        end = total_samples

        window = signal[start:end]
        window = window.unsqueeze(0)

        prediction = classifier.classify_batch(window)
        lang_id = prediction[3]
        preds.append(lang_id[0])
    else:
        # Iterate over the audio file with a window size == WINDOW_SIZE and a stride == STRIDE
        for i in range(0, total_samples - window_size_samples + 1, stride_size_samples):
            start = i
            end = i + window_size_samples

            if end > total_samples:
                end = total_samples

            window = signal[start:end]
            window = window.unsqueeze(0)

            prediction = classifier.classify_batch(window)
            lang_id = prediction[3]
            preds.append(lang_id[0])

    return most_frequent(preds)  # Return most frequent language in the audio file

def copy_audio_to_lang_folder(path, lang, audio_file):
    langPath = os.path.join(path, lang.strip())
    os.makedirs(langPath, exist_ok=True)
    vtt_file = [file for file in os.listdir(path) if file.startswith(os.path.basename(audio_file[:-4])) and file.endswith(".vtt")]
    if vtt_file:
        shutil.move(f'{audio_file}', f'{os.path.join(langPath, os.path.basename(audio_file))}')
        shutil.move(f'{os.path.join(path, vtt_file[0])}', f'{os.path.join(langPath, vtt_file[0])}')
        return True
    else:
        # If no VTT file found, just move the audio file
        shutil.move(f'{audio_file}', f'{os.path.join(langPath, os.path.basename(audio_file))}')
        return False

def process_audio_files():
    """Process audio files and organize them by detected language"""
    global language_classifier
    
    path = "audio-and-captions"
    audio_list = glob.glob(f'{path}/*.mp3')
    
    results = []
    if len(audio_list)==0:
        print("Folder doesn't contain audio files")
        return {"status": "error", "message": "Folder doesn't contain audio files"}
    else:
        audio_list = natsorted(audio_list, alg=ns.IGNORECASE)
        for audio_file in tqdm(
                audio_list,
                total=len(audio_list),
                desc=f"Processing",
                ):
            print(f"Processing {audio_file}")
            try:
                lang = detect_lang(audio_file, language_classifier)
                print(f"  Detected language: {lang}")
                language_code = lang.split(":")[1]
                vtt_found = copy_audio_to_lang_folder(path, language_code, audio_file)
                results.append({
                    "file": audio_file, 
                    "language": language_code, 
                    "vtt_found": vtt_found,
                    "status": "success"
                })
            except Exception as e:
                print(f"  Error processing {audio_file}: {str(e)}")
                results.append({"file": audio_file, "status": "error", "message": str(e)})
    
    return {"status": "completed", "results": results}

# Initialize the model at module level before Flask starts
print("Initializing language detection model...")
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {device}")

# Global variable to track initialization status
model_initialized = False

# Function to initialize the model
def initialize_model():
    global language_classifier, model_initialized
    try:
        language_classifier = EncoderClassifier.from_hparams(
            source="speechbrain/lang-id-voxlingua107-ecapa",
            savedir="lid-model",
            run_opts={"device": device}
        )
        language_classifier.hparams.label_encoder.ignore_len()
        print("Model loaded successfully!")
        model_initialized = True
    except Exception as e:
        print(f"Error loading model: {e}")
        model_initialized = False

# Start model initialization in a thread so we don't block app startup
init_thread = threading.Thread(target=initialize_model)
init_thread.daemon = True
init_thread.start()

@app.route('/health', methods=['GET'])
def health_check():
    global model_initialized
    if not model_initialized:
        return jsonify({"status": "initializing", "message": "Model is still initializing"})
    return jsonify({"status": "healthy", "model_loaded": True})

@app.route('/process', methods=['POST'])
def process_audio():
    global is_processing, processing_results, model_initialized
    
    # Check if model is loaded
    if not model_initialized:
        return jsonify({"status": "error", "message": "Model is still initializing, please try again later"})
    
    # Check if process is already running
    if is_processing:
        return jsonify({"status": "processing", "message": "Audio processing is already in progress"})
    
    # Start processing in a separate thread
    def process_thread():
        global is_processing, processing_results
        try:
            processing_results = process_audio_files()
            is_processing = False
        except Exception as e:
            processing_results = {"status": "error", "message": str(e)}
            is_processing = False
    
    is_processing = True
    thread = threading.Thread(target=process_thread)
    thread.start()
    
    return jsonify({"status": "started", "message": "Audio processing has started"})

@app.route('/status', methods=['GET'])
def check_status():
    global is_processing, processing_results
    
    if is_processing:
        return jsonify({"status": "processing", "message": "Audio processing is in progress"})
    elif processing_results is not None:
        return jsonify(processing_results)
    else:
        return jsonify({"status": "idle", "message": "No processing has been initiated"})

if __name__ == '__main__':
    print("Starting language detection server on port 3002")
    app.run(host='0.0.0.0', port=3002, debug=False)