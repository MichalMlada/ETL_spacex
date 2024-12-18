import os
import requests
import json

# Configurations
SPACEX_API_URL = "https://api.spacexdata.com/v4/"
DATASETS = [
    {
        'table_name': 'launches',
        'source_url': 'launches',
        'save_path': 'data'
    },
    {
        'table_name': 'payloads',
        'source_url': 'payloads',
        'save_path': 'data'
    }
]

def fetch_and_save_data(dataset):
    """
    Fetch data from API and save it as JSON to the specified directory.
    """
    table_name = dataset['table_name']
    source_url = dataset['source_url']
    save_path = dataset['save_path']

    # Create the save directory if it doesn't exist
    os.makedirs(save_path, exist_ok=True)

    # Fetch the data
    response = requests.get(SPACEX_API_URL + source_url)
    if response.status_code == 200:
        data = response.json()
        
        # Save the data to a JSON file
        file_path = os.path.join(save_path, f"{table_name}.json")
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        
        print(f"Data saved for table '{table_name}' at {file_path}")
    else:
        print(f"Failed to fetch data for table '{table_name}'. HTTP Status: {response.status_code}")

def main():
    for dataset in DATASETS:
        fetch_and_save_data(dataset)

if __name__ == "__main__":
    main()
