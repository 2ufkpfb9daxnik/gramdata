import json
import os

def convert_ngram_to_json(input_file, output_file):
    """
    Convert tab-separated n-gram frequency file to JSON format
    
    Args:
        input_file: Path to the input TSV file
        output_file: Path to the output JSON file
    """
    print(f"Processing {input_file}...")
    
    # Initialize data dictionary
    data = {}
    
    try:
        # Read input file
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('//'):
                    continue
                    
                # Split the line into frequency and n-gram
                parts = line.split('\t', 1)
                if len(parts) != 2:
                    continue
                    
                freq, gram = parts
                try:
                    freq = int(freq)
                    data[gram] = freq
                except ValueError:
                    print(f"Warning: Could not parse frequency in line: {line}")
    
        # Write to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        print(f"Successfully created {output_file} with {len(data)} entries")
    
    except Exception as e:
        print(f"Error processing {input_file}: {e}")
        
def main():
    base_dir = r"d:\gramdata"
    
    # Define files to process
    files = [
        ("wikipedia.hiragana-asis.1gram.txt", "wikikana1gram.json"),
        ("wikipedia.hiragana-asis.2gram.txt", "wikikana2gram.json"),
        ("wikipedia.hiragana-asis.3gram.txt", "wikikana3gram.json")
    ]
    
    # Process each file
    for input_file, output_file in files:
        input_path = os.path.join(base_dir, input_file)
        output_path = os.path.join(base_dir, output_file)
        
        if os.path.exists(input_path):
            convert_ngram_to_json(input_path, output_path)
        else:
            print(f"Warning: Input file not found: {input_path}")

if __name__ == "__main__":
    main()