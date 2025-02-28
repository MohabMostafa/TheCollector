step-1 [the-collector environment]:
get urls from keywords using _get_urls_from_keywords.py

step-2 [the-collector environment]:
remove songs from url list using _music_detector.py

step-3 [the-collector environment]:
download audio from collected urls using _get_urls_and_download.py

step-4 [lang-id environment]
filter audio files by targeting only target language using _lang_id.py

step-5 [dialect_id environment]
filter files based on dialect ECA/MSA using _dialect_id.py

step-6 [the-collector environment]
extract arabic and mixed utterances from downloaded captions using _mixed_arabic_extractor.py

step-7 [the-collector environment]
prepare data using _prepare_data.py