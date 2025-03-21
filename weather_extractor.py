import os
import json
from datetime import datetime
import requests
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
import time

class WeatherExtractor:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        if not self.api_key:
            raise ValueError("OpenWeather API key not found in environment variables")
        
        self.base_url = "http://api.openweathermap.org/data/2.5"
        self.geolocator = Nominatim(user_agent="weather_etl_app")
        
        # List of cities to track
        self.cities = [
            "New York, USA",
            "London, UK",
            "Tokyo, Japan",
            "Sydney, Australia",
            "Paris, France",
            "Mumbai, India",
            "Dubai, UAE",
            "Singapore",
            "San Francisco, USA",
            "Toronto, Canada"
        ]
        
        # Cache for geocoding results
        self.geocoding_cache = {}

    def get_coordinates(self, city_name):
        """Get latitude and longitude for a city with caching"""
        if city_name in self.geocoding_cache:
            return self.geocoding_cache[city_name]
        
        try:
            location = self.geolocator.geocode(city_name)
            if location:
                coords = {
                    'lat': location.latitude,
                    'lon': location.longitude,
                    'name': city_name
                }
                self.geocoding_cache[city_name] = coords
                return coords
            else:
                print(f"Could not find coordinates for {city_name}")
                return None
        except Exception as e:
            print(f"Error getting coordinates for {city_name}: {str(e)}")
            return None

    def get_current_weather(self, lat, lon):
        """Get current weather data for given coordinates"""
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'  # Use metric units
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {str(e)}")
            return None

    def get_air_quality(self, lat, lon):
        """Get air quality data for given coordinates"""
        url = f"{self.base_url}/air_pollution"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching air quality data: {str(e)}")
            return None

    def extract_and_save(self, output_dir='../data/raw'):
        """Extract weather and air quality data for all cities and save to JSON"""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        all_data = []
        
        for city in self.cities:
            print(f"Fetching data for {city}...")
            coords = self.get_coordinates(city)
            
            if coords:
                # Add delay to respect API rate limits
                time.sleep(1)
                
                weather_data = self.get_current_weather(coords['lat'], coords['lon'])
                if weather_data:
                    time.sleep(1)  # Additional delay between API calls
                    air_quality = self.get_air_quality(coords['lat'], coords['lon'])
                    
                    if air_quality:
                        city_data = {
                            'city': coords['name'],
                            'latitude': coords['lat'],
                            'longitude': coords['lon'],
                            'timestamp': datetime.now().isoformat(),
                            'weather': weather_data,
                            'air_quality': air_quality
                        }
                        all_data.append(city_data)
                    else:
                        print(f"Failed to get air quality data for {city}")
                else:
                    print(f"Failed to get weather data for {city}")
        
        if all_data:
            output_file = os.path.join(output_dir, f'weather_data_{timestamp}.json')
            with open(output_file, 'w') as f:
                json.dump(all_data, f, indent=2)
            print(f"Data saved to {output_file}")
            return output_file
        
        print("No data was collected")
        return None

if __name__ == "__main__":
    extractor = WeatherExtractor()
    extractor.extract_and_save() 