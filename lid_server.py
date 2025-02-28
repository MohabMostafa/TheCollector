from flask import Flask, request, jsonify
from speechbrain.pretrained import EncoderClassifier
import torchaudio
import torch
import os
import re
from collections import Counter

app = Flask(__name__)

def most_frequent(lst):
    return Counter(lst).most_common(1)[0][0]

def preprocess(signal: torch.Tensor, sr: int) -> torch.Tensor:
    CHANNELS = 1
    SAMPLE_RATE = 16000
    # Resample if necessary
    if sr != SAMPLE_RATE:
        resample = torchaudio.transforms.Resample(orig_freq=sr, new_freq=SAMPLE_RATE)
        signal = resample(signal)
    # Convert to mono if not already
    if signal.shape[0] != CHANNELS:
        signal = torch.mean(signal, dim=0, keepdim=True)
    return signal

def detect_lang(path: str, classifier: EncoderClassifier) -> str:
    signal, sr = torchaudio.load(path, num_frames=100000000)
    signal = preprocess(signal, sr).squeeze(0)
    total_samples = len(signal)
    WINDOW_SIZE = 30  # seconds
    STRIDE = 30       # seconds
    window_size = int(16000 * WINDOW_SIZE)
    stride_size = int(16000 * STRIDE)
    preds = []
    if total_samples <= window_size:
        window = signal.unsqueeze(0)
        prediction = classifier.classify_batch(window)
        preds.append(prediction[3][0])
    else:
        for i in range(0, total_samples - window_size + 1, stride_size):
            window = signal[i:i+window_size].unsqueeze(0)
            prediction = classifier.classify_batch(window)
            preds.append(prediction[3][0])
    return most_frequent(preds)

# Load the language identification model once at startup.
classifier = EncoderClassifier.from_hparams(
    source="speechbrain/lang-id-voxlingua107-ecapa",
    savedir="lid-model",
    run_opts={"device": "cuda"}
)
classifier.hparams.label_encoder.ignore_len()

@app.route('/detect', methods=['POST'])
def detect():
    if 'audio' not in request.files:
        return jsonify({"error": "No audio file provided"}), 400
    audio_file = request.files['audio']
    temp_path = "temp_audio.mp3"
    audio_file.save(temp_path)
    try:
        language = detect_lang(temp_path, classifier)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    return jsonify({"language": language})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3002)
