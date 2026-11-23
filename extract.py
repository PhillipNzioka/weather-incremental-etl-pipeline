import time
from datetime import date
import requests
from config import (BACKOFF_DELAYS, GEOCODE_URL, ARCHIVE_URL, DAILY_VARIABLES, START_DATE)


def get_json(url: str, params: dict = None, timeout: float = 5.0) -> dict:
    """
    Fetches JSON data from a URL with a timeout and exponential backoff retries.
    Retries after 1s, 2s, and 4s on request failures or server errors.
    """
    # backoff_delays = [1, 2, 4]
    
    for attempt, delay in enumerate(BACKOFF_DELAYS):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            
            # Raise an HTTPError for 4xx or 5xx status codes
            response.raise_for_status()
            
            return response.json()
            
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            
    # Final attempt after backoff loops are exhausted
    final_response = requests.get(url, params=params, timeout=timeout)
    final_response.raise_for_status()
    return final_response.json()

def get_geocode(town):
# Example usage with your URL:
    url = GEOCODE_URL
    params = {
        "name": town,
        "countryCode": "KE",
        "count": 1
    }

    data = get_json(url, params)
    payload = data.get("results")
    hit = payload[0]
    return {
        "city_id": hit["id"],
        "name": hit["name"],
        "county": hit.get("admin1"),
        "latitude": hit["latitude"],
        "longitude": hit["longitude"],
        "elevation": hit.get("elevation"),
        "population": hit.get("population"),
    }
    

def fetch_weather(city, start_date, end_date):
    payload = get_json(ARCHIVE_URL, 
        {
            "latitude": city["latitude"],
	        "longitude": city["longitude"],
	        "start_date": start_date.isoformat(), 
	        "end_date": end_date.isoformat(),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": "Africa/Nairobi"
        })
    return payload

towns = ["Nairobi", "Kisumu", "Mombasa", "Nakuru"]
for town in towns:
    print(f"City: {town}")
    print(fetch_weather(get_geocode(town), START_DATE, date(2025, 1, 3)))
    print()