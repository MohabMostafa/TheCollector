from speechbrain.pretrained import EncoderClassifier
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
    signal, sr = torchaudio.load(path, num_frames=100000000)
    signal = preprocess(signal=signal, sr=sr)
    signal = signal.squeeze(0)

    total_samples = len(signal)
    WINDOW_SIZE = 30
    STRIDE = 30
    window_size_samples = int(16_000 * WINDOW_SIZE)
    stride_size_samples = int(16_000 * STRIDE)
    preds = []
    # print("loaded audio file")
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
    shutil.move(f'{audio_file}',f'{os.path.join(langPath, os.path.basename(audio_file))}')
    shutil.move(f'{os.path.join(path, vtt_file[0])}',f'{os.path.join(langPath, vtt_file[0])}')

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

def main():
    language_id = EncoderClassifier.from_hparams(
            source="speechbrain/lang-id-voxlingua107-ecapa",
            savedir="lid-model",
            run_opts={"device":"cuda"})
    language_id.hparams.label_encoder.ignore_len()
    path = "audio-and-captions"
    audio_list = glob.glob(f'{path}/*.mp3')
    if len(audio_list)==0:
        print("Folder doesn't contain audio files")
    else:
        audio_list = natsorted(audio_list, alg=ns.IGNORECASE)
        for audio_file in tqdm(
                audio_list,
                total=len(audio_list),
                desc=f"Processing",
                ):
            print(audio_file)
            lang = detect_lang(audio_file, language_id)
            # print(lang)
            copy_audio_to_lang_folder(path, lang.split(":")[1], audio_file)

if __name__ == "__main__":
    main()