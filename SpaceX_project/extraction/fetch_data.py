# extraction/fetch_data.py
import requests

def fetch_data(api_url):
    """Fetch data from the SpaceX API."""
    try:
        print(f"Fetching data from {api_url}...")
        response = requests.get(api_url)
        response.raise_for_status()
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise
