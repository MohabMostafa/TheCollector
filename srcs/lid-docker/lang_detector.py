from speechbrain.inference import EncoderClassifier
from natsort import natsorted, ns
import shutil
import glob
import os
import torchaudio
import torch
from tqdm import tqdm
from collections import Counter

def most_frequent(List):
    occurence_count = Counter(List)

    # Get most common element in a list
    most_frequent_lang = occurence_count.most_common(1)[0][0]

    return most_frequent_lang

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

def process_audio_files(language_id=None):
    """Process audio files and organize them by detected language"""
    # If model wasn't passed, load it (fallback)
    if language_id is None:
        language_id = EncoderClassifier.from_hparams(
                source="speechbrain/lang-id-voxlingua107-ecapa",
                savedir="lid-model",
                run_opts={"device":"cuda" if torch.cuda.is_available() else "cpu"})
        language_id.hparams.label_encoder.ignore_len()
    
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
                lang = detect_lang(audio_file, language_id)
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