import glob
import re
from collections import Counter

# Folder where your VTT files are stored
folder_path = 'audio-and-captions/Arabic/MSA'

# Pattern to match words, ignoring punctuation and numbers
word_pattern = re.compile(r'\b\w+\b', re.UNICODE)

# Initialize a Counter object to keep track of word frequencies
word_frequencies = Counter()

# Loop through all VTT files in the folder
for file_path in glob.glob(f'{folder_path}/*.vtt'):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        
        # Skip the initial metadata lines (WEBVTT, Kind, Language)
        lines = content.splitlines()
        text_lines = [line for line in lines if not line.startswith(('WEBVTT', 'Kind:', 'Language:'))]
        
        # Join the text lines back into a single string
        text_content = '\n'.join(text_lines)
        
        # Find all words in the text content
        words = word_pattern.findall(text_content)
        # Update word frequencies, filtering out purely numeric tokens
        word_frequencies.update(word for word in words if not word.isdigit())

# Path for the output text file
output_txt_file_path = 'unique_words_with_frequencies.txt'

# Write the unique words and their frequencies to a text file
with open(output_txt_file_path, 'w', encoding='utf-8') as output_file:
    for word, frequency in word_frequencies.most_common():
        output_file.write(f'{word}: {frequency}\n')

print(f'Unique words with frequencies have been written to {output_txt_file_path}')
