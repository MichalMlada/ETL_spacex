# extraction/save_data.py
import os
import json

def save_data_to_file(data, save_path, table_name):
    """Save fetched JSON data to a file in the specified path."""
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    file_path = os.path.join(save_path, f"{table_name}.json")
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {file_path}")
