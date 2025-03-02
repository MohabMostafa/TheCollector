# The Collector `(dpipe)` <!-- omit in toc -->

## Table of Contents <!-- omit in toc -->

- [Dependencies](#dependencies)
- [Installation](#installation)
  - [Step 1: Collect URLs (the-collector environment)](#step-1-collect-urls-the-collector-environment)
  - [Step 2: Filter Music URLs (the-collector environment)](#step-2-filter-music-urls-the-collector-environment)
  - [Step 3: Download Audio (the-collector environment)](#step-3-download-audio-the-collector-environment)
  - [Step 4: Filter by Language (lang-id environment)](#step-4-filter-by-language-lang-id-environment)
  - [Step 5: Filter by Dialect (dialect\_id environment)](#step-5-filter-by-dialect-dialect_id-environment)
  - [Step 6: Extract Arabic Utterances (the-collector environment)](#step-6-extract-arabic-utterances-the-collector-environment)
  - [Step 7: Prepare Data (the-collector environment)](#step-7-prepare-data-the-collector-environment)
- [Build Documentation](#build-documentation)
- [References](#references)

---

## Dependencies

To set up the required environment:

```bash
conda create -n the-collector python=3.12 -y
conda activate the-collector
python -m pip install -U pip
pip install -r requirements.txt
```

---

## Installation

### Step 1: Collect URLs (the-collector environment)

Retrieve URLs based on keywords using:

```bash
python _get_urls_from_keywords.py
```

### Step 2: Filter Music URLs (the-collector environment)

Remove songs from the collected URLs using:

```bash
python _music_detector.py
```

### Step 3: Download Audio (the-collector environment)

Download audio from the filtered URLs using:

```bash
python _get_urls_and_download.py
```

### Step 4: Filter by Language (lang-id environment)

Filter audio files to retain only the target language using:

```bash
python _lang_id.py
```

### Step 5: Filter by Dialect (dialect_id environment)

Classify files by dialect (ECA/MSA) using:

```bash
python _dialect_id.py
```

### Step 6: Extract Arabic Utterances (the-collector environment)

Extract Arabic and mixed-language utterances from captions using:

```bash
python _mixed_arabic_extractor.py
```

### Step 7: Prepare Data (the-collector environment)

Prepare the final dataset using:

```bash
python _prepare_data.py
```

---

## Build Documentation

To build the documentation, run:

```bash
docker compose up -d dpipe-mkdocs
```

Then, visit [localhost:5001](http://localhost:5001) in your browser.

---

## References

- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
