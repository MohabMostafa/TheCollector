# The Collector <!-- omit in toc -->

## Table of contents <!-- omit in toc -->

- [Dependencies](#dependencies)
- [Installation](#installation)
  - [Step 1 \[the-collector environment\]](#step-1-the-collector-environment)
  - [Step 2 \[the-collector environment\]](#step-2-the-collector-environment)
  - [Step 3 \[the-collector environment\]](#step-3-the-collector-environment)
  - [Step 4 \[lang-id environment\]](#step-4-lang-id-environment)
  - [Step 5 \[dialect\_id environment\]](#step-5-dialect_id-environment)
  - [Step 6 \[the-collector environment\]](#step-6-the-collector-environment)
  - [Step 7 \[the-collector environment\]](#step-7-the-collector-environment)
- [References](#references)

## Dependencies

```bash
conda create -n thecollector python=3.12 -y

conda activate thecollector
python -m pip install -U pip

pip install -r requirements.txt
```

## Installation

### Step 1 [the-collector environment]

- get urls from keywords using _get_urls_from_keywords.py

### Step 2 [the-collector environment]

- remove songs from url list using _music_detector.py

### Step 3 [the-collector environment]

- download audio from collected urls using _get_urls_and_download.py

### Step 4 [lang-id environment]

- filter audio files by targeting only target language using _lang_id.py

### Step 5 [dialect_id environment]

- filter files based on dialect ECA/MSA using _dialect_id.py

### Step 6 [the-collector environment]

- extract arabic and mixed utterances from downloaded captions using _mixed_arabic_extractor.py

### Step 7 [the-collector environment]

- prepare data using _prepare_data.py

## References

- TBC
