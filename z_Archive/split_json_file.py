import json
import os

def split_json_file(input_file, output_directory):
    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Read the input file
    with open(input_file, 'r') as f:
        content = f.read()

    # Split the content by the separator
    json_strings = content.split('||||')

    # Process each JSON string
    for i, json_string in enumerate(json_strings, 1):
        # Skip empty strings
        if not json_string.strip():
            continue

        # Parse the JSON
        try:
            json_data = json.loads(json_string.strip())
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON in entry {i}: {e}")
            continue

        # Write to individual file
        output_file = os.path.join(output_directory, f'test_json{i:03d}.json')
        with open(output_file, 'w') as f:
            json.dump(json_data, f, indent=2)

        print(f"Created file: {output_file}")

if __name__ == "__main__":
    input_file = r"C:\Users\kroy2\Documents\python\projects\json_processor\data\json_test_cases.txt"  # Name of the file containing all JSON objects
    output_directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\json_test_files"  # Directory to store individual JSON files

    split_json_file(input_file, output_directory)
    print("Splitting complete!")