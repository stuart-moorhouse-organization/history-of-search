#!/usr/bin/env python3
"""
Clean Shakespeare data by removing entries with less than 4 words in text_entry.
"""

import json
import sys

def count_words(text):
    """Count words in a text string."""
    if not text or not isinstance(text, str):
        return 0
    return len(text.strip().split())

def clean_shakespeare_data(input_file, output_file):
    """
    Clean Shakespeare data by removing entries with less than 4 words.
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output cleaned JSON file
    """
    total_entries = 0
    kept_entries = 0
    removed_entries = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            lines = infile.readlines()
            
            i = 0
            while i < len(lines):
                # Each entry consists of an index line followed by a data line
                if i + 1 >= len(lines):
                    break
                    
                index_line = lines[i].strip()
                data_line = lines[i + 1].strip()
                
                total_entries += 1
                
                try:
                    # Parse the data line
                    data = json.loads(data_line)
                    text_entry = data.get('text_entry', '')
                    word_count = count_words(text_entry)
                    
                    # Keep entries with 4 or more words
                    if word_count >= 4:
                        # Update the line_id to maintain sequential order
                        data['line_id'] = kept_entries + 1
                        
                        # Write both index and data lines
                        # Update the index line with new ID
                        index_data = json.loads(index_line)
                        index_data['index']['_id'] = kept_entries
                        
                        outfile.write(json.dumps(index_data) + '\n')
                        outfile.write(json.dumps(data) + '\n')
                        kept_entries += 1
                    else:
                        removed_entries += 1
                        print(f"Removed entry (ID {data.get('line_id', 'unknown')}): '{text_entry}' ({word_count} words)")
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing line {i}: {e}")
                    continue
                
                i += 2  # Move to next entry (skip index and data line)
    
    print(f"\nCleaning complete:")
    print(f"Total entries processed: {total_entries}")
    print(f"Entries kept: {kept_entries}")
    print(f"Entries removed: {removed_entries}")
    print(f"Removal percentage: {(removed_entries/total_entries)*100:.1f}%")

if __name__ == "__main__":
    input_file = "../shakespeare.json"
    output_file = "../shakespeare_cleaned.json"
    
    print("Cleaning Shakespeare data...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    
    clean_shakespeare_data(input_file, output_file)
    print(f"Cleaned data saved to {output_file}")