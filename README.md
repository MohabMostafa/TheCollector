```
 _____ _            ____      _ _           _             
|_   _| |__   ___  / ___|___ | | | ___  ___| |_ ___  _ __ 
  | | | '_ \ / _ \| |   / _ \| | |/ _ \/ __| __/ _ \| '__|
  | | | | | |  __/| |__| (_) | | |  __/ (__| || (_) | |   
  |_| |_| |_|\___| \____\___/|_|_|\___|\___|\__\___/|_|
  Author: Mohab Mostafa
```
The **Collector** is a modular framework for collecting, filtering, and preparing Arabic speech datasets.  
It provides tools for downloading audio, removing music, identifying language and dialect, and preparing segmented speech datasets for both **Arabic-only** and **code-switched** utterances.  

🔗 **Repository:** [TheCollector](https://github.com/MohabMostafa/TheCollector.git)

---

## 🚀 Features
- Collect audio and captions automatically from YouTube using `yt-dlp`.
- Remove music-based content for cleaner speech datasets.
- Identify target language using [SpeechBrain](https://speechbrain.github.io/).
- Detect dialects (ECA/MSA) using [Hugging Face Transformers](https://huggingface.co/transformers/).
- Extract Arabic-only vs. code-switched utterances.
- Generate final segmented audio datasets ready for research or training.

---

## 📦 Installation

### 1. Clone the repository
```bash
git clone https://github.com/MohabMostafa/TheCollector.git
cd TheCollector
```

### 2. Create Conda Environments
Each environment is based on **Python 3.10**.  

#### The Collector (data collection & preprocessing)
```bash
conda create -n the-collector python=3.10
conda activate the-collector
pip install -r requirements/the-collector.txt
```

#### Language Identification
```bash
conda create -n lang-id python=3.10
conda activate lang-id
pip install -r requirements/lang-id.txt
```

#### Dialect Identification
```bash
conda create -n dialect-id python=3.10
conda activate dialect-id
pip install -r requirements/dialect-id.txt
```

---

## ⚡ Usage – 7 Steps

To prepare a dataset of segmented audios with their transcriptions, follow these **7 steps**.  

Before starting, make sure you have **keywords.txt** inside the `keywords/` folder (containing one or more keywords).  

---

### **Step 1 – Get URLs from Keywords**  
**Environment:** `the-collector`  
```bash
python srcs/original_scripts/_get_urls_from_keywords.py
```
- Creates `urls.txt` inside `url_list/` with collected YouTube URLs.

---

### **Step 2 – Remove Music Videos**  
**Environment:** `the-collector`  
```bash
python srcs/original_scripts/_music_detector.py
```
- Cleans `urls.txt` by removing links to music videos.

---

### **Step 3 – Download Audio & Captions**  
**Environment:** `the-collector`  
```bash
python srcs/original_scripts/_get_urls_and_download.py
```
- Downloads audio and target language captions (`.vtt` files, Arabic hardcoded).  
- Saves results in `audio-and-captions/`.

---

### **Step 4 – Language Identification**  
**Environment:** `lang-id`  
```bash
python srcs/original_scripts/_lang_id.py
```
- Detects language of each audio file.  
- Moves valid files into `audio-and-captions/<Language>/` with their captions.

---

### **Step 5 – Dialect Identification (ECA/MSA)**  
**Environment:** `dialect-id`  
```bash
python srcs/original_scripts/_dialect_id.py
```
- Splits files into `ECA/` and `MSA/` subfolders under the language folder.  

---

### **Step 6 – Extract Arabic & Mixed Utterances**  
**Environment:** `the-collector`  
```bash
python srcs/original_scripts/_mixed_arabic_extractor.py [ECA|MSA]
```
- Produces:  
  - `arabic-only-ECA/` (or `arabic-only-MSA/`) → captions with only Arabic.  
  - `mixedlanguage-ECA/` (or `mixedlanguage-MSA/`) → captions with code-switched utterances.  

---

### **Step 7 – Prepare Segmented Data**  
**Environment:** `the-collector`  
```bash
python srcs/original_scripts/_prepare_data.py [ECA|MSA]
```
- Creates segmented audios:  
  - `output-folder-ECA/` → segmented audios for Arabic-only & mixed language (ECA).  
  - `output-folder-MSA/` → segmented audios for Arabic-only & mixed language (MSA).  

✅ Final result: a ready-to-use **speech dataset** with both **Arabic-only** and **code-switched** segments.

---

## 📂 Project Structure
```
TheCollector/
│
├── requirements/             # requirements files for each env
│   ├── the-collector.txt
│   ├── lang-id.txt
│   └── dialect-id.txt
│
├── keywords/                 # keywords for searching
│   └── keywords.txt
│
├── url_list/                 # collected urls
│
├── audio-and-captions/       # downloaded audio + captions
│
├── srcs/original_scripts/    # main scripts for processing
│
└── README.md                 # this file
```

---

## 🛠 Dependencies
Key libraries used across environments:
- **Data Collection**: `yt-dlp`, `pydub`, `mutagen`
- **Language ID**: `speechbrain`, `torchaudio`, `sentencepiece`
- **Dialect ID**: `transformers`, `tokenizers`, `torch`

All dependencies are pinned in their respective `requirements.txt` files.

---

## 🔮 Enhancements & Future Work
- Automating the **7-step pipeline** using [Dagster](https://dagster.io/) for reproducible data pipelines.  
- Adding configuration files to avoid hardcoding (e.g., language codes).  
- Support for more dialects and additional languages.  
- Improved error handling and logging.  
- Dockerized deployment for easier setup.  

---

## 📖 License
MIT License. See [LICENSE](LICENSE) for details.

---

## 🙌 Acknowledgments
- [SpeechBrain](https://speechbrain.github.io/) for language ID models.  
- [Hugging Face](https://huggingface.co/) for Transformer-based dialect ID.  
- Open-source community for toolkits that made this project possible.
- [Ibrahim Amin](https://github.com/IbrahimAmin1) for his help and support.
