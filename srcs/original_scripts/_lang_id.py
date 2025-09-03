from speechbrain.inference import EncoderClassifier
from natsort import natsorted, ns
import shutil
import glob
import os
import torchaudio
import torch
from tqdm import tqdm
from collections import Counter
import stat
import warnings

# -------------------------------------------------------------------
# Suppress known warnings from torchaudio & speechbrain
# -------------------------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning, module="torchaudio._backend")
warnings.filterwarnings("ignore", category=UserWarning, module="speechbrain.utils.torch_audio_backend")
warnings.filterwarnings("ignore", category=FutureWarning, module="speechbrain.utils.autocast")
warnings.filterwarnings("ignore", message="In 2.9, this function's implementation will be changed")

# -------------------------------------------------------------------
# AMP patch for SpeechBrain (fixes deprecated torch.cuda.amp.custom_fwd)
# -------------------------------------------------------------------
if hasattr(torch.cuda.amp, "custom_fwd"):
    from torch.amp import custom_fwd, custom_bwd
    torch.cuda.amp.custom_fwd = lambda *args, **kwargs: custom_fwd(*args, device_type="cuda", **kwargs)
    torch.cuda.amp.custom_bwd = lambda *args, **kwargs: custom_bwd(*args, device_type="cuda", **kwargs)


def most_frequent(List):
    occurence_count = Counter(List)
    return occurence_count.most_common(1)[0][0]

def detect_lang(path: str, classifier: EncoderClassifier) -> str:
    """Detect language from audio file using a classifier."""
    try:
        from torchcodec.decoders import AudioDecoder
        signal, sr = AudioDecoder(path).decode()
    except ImportError:
        signal, sr = torchaudio.load(path, num_frames=10_000_000)

    signal = preprocess(signal=signal, sr=sr)
    signal = signal.squeeze(0)

    total_samples = len(signal)
    WINDOW_SIZE = 30
    STRIDE = 30
    window_size_samples = int(16_000 * WINDOW_SIZE)
    stride_size_samples = int(16_000 * STRIDE)
    preds = []

    if total_samples <= window_size_samples:
        # Process whole signal
        window = signal.unsqueeze(0)
        prediction = classifier.classify_batch(window)
        lang_id = prediction[3]
        preds.append(lang_id[0])
    else:
        # Process in windows
        for i in range(0, total_samples - window_size_samples + 1, stride_size_samples):
            start = i
            end = min(i + window_size_samples, total_samples)
            window = signal[start:end].unsqueeze(0)
            prediction = classifier.classify_batch(window)
            lang_id = prediction[3]
            preds.append(lang_id[0])

    return most_frequent(preds)


def copy_audio_to_lang_folder(path, lang, audio_file):
    """Move audio and matching VTT into language-specific folder."""
    langPath = os.path.join(path, lang.strip())
    os.makedirs(langPath, exist_ok=True)

    vtt_file = [
        file for file in os.listdir(path)
        if file.startswith(os.path.basename(audio_file[:-4])) and file.endswith(".vtt")
    ]

    shutil.move(audio_file, os.path.join(langPath, os.path.basename(audio_file)))
    if vtt_file:
        shutil.move(os.path.join(path, vtt_file[0]), os.path.join(langPath, vtt_file[0]))


def preprocess(signal: torch.tensor, sr: int) -> torch.tensor:
    """Resample and convert audio to mono 16kHz."""
    SAMPLE_RATE = 16_000

    if sr != SAMPLE_RATE:
        resample_transform = torchaudio.transforms.Resample(orig_freq=sr, new_freq=SAMPLE_RATE)
        resampled_waveform = resample_transform(signal)
    else:
        resampled_waveform = signal

    if resampled_waveform.shape[0] != 1:
        monochannel_waveform = torch.mean(resampled_waveform, dim=0, keepdim=True)
    else:
        monochannel_waveform = resampled_waveform

    return monochannel_waveform


def setup_model_directory():
    """Create a writable model directory and set proper permissions."""
    home_dir = os.path.expanduser("~")
    model_dir = os.path.join(home_dir, "speechbrain_models", "lid-model")
    os.makedirs(model_dir, exist_ok=True)

    try:
        os.chmod(model_dir, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
    except Exception as e:
        print(f"Warning: Could not set permissions on {model_dir}: {e}")

    return model_dir


def main():
    # Ensure soundfile backend is available
    try:
        import soundfile
        print("Using torchaudio with soundfile backend (dispatcher mode)")
    except ImportError:
        print("Soundfile not installed. torchaudio will fallback to default backend.")

    model_dir = setup_model_directory()

    # Load classifier
    try:
        language_id = EncoderClassifier.from_hparams(
            source="speechbrain/lang-id-voxlingua107-ecapa",
            savedir=model_dir,
            run_opts={"device": "cuda"},
        )
        language_id.hparams.label_encoder.ignore_len()
    except Exception as e:
        print(f"Error loading model on CUDA: {e}")
        print("Trying with CPU...")
        try:
            language_id = EncoderClassifier.from_hparams(
                source="speechbrain/lang-id-voxlingua107-ecapa",
                savedir=model_dir,
                run_opts={"device": "cpu"},
            )
            language_id.hparams.label_encoder.ignore_len()
        except Exception as e2:
            print(f"Failed to load model on CPU as well: {e2}")
            return

    path = "audio-and-captions"
    audio_list = glob.glob(f"{path}/*.mp3")
    if not audio_list:
        print("Folder doesn't contain audio files")
        return

    audio_list = natsorted(audio_list, alg=ns.IGNORECASE)

    success, failed = 0, 0
    with tqdm(audio_list, total=len(audio_list), desc="Processing") as pbar:
        for audio_file in pbar:
            pbar.set_postfix_str(os.path.basename(audio_file))
            try:
                lang = detect_lang(audio_file, language_id)
                copy_audio_to_lang_folder(path, lang.split(":")[1], audio_file)
                success += 1
            except Exception as e:
                failed += 1
                pbar.set_postfix_str(f"Error: {os.path.basename(audio_file)}")
                continue

    print(f"\nFinished: {success} processed, {failed} failed.")


if __name__ == "__main__":
    main()
