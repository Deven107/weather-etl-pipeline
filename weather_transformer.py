import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

class WeatherTransformer:
    def __init__(self, input_dir='../data/raw', output_dir='../data/processed'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def get_latest_data_file(self):
        """Get the most recent weather data file"""
        files = [f for f in os.listdir(self.input_dir) if f.startswith('weather_data_')]
        if not files:
            return None
        return os.path.join(self.input_dir, sorted(files)[-1])

    def _process_weather_data(self, data):
        """Transform weather data into a structured format"""
        weather_records = []
        
        for city_data in data:
            weather = city_data['weather']
            record = {
                'city': city_data['city'],
                'latitude': city_data['latitude'],
                'longitude': city_data['longitude'],
                'timestamp': pd.to_datetime(city_data['timestamp']),
                'temperature': weather['main']['temp'],
                'feels_like': weather['main']['feels_like'],
                'humidity': weather['main']['humidity'],
                'pressure': weather['main']['pressure'],
                'wind_speed': weather['wind']['speed'],
                'wind_direction': weather['wind'].get('deg', np.nan),
                'clouds_percent': weather['clouds']['all'],
                'weather_main': weather['weather'][0]['main'],
                'weather_description': weather['weather'][0]['description'],
                'sunrise': pd.to_datetime(weather['sys']['sunrise'], unit='s'),
                'sunset': pd.to_datetime(weather['sys']['sunset'], unit='s')
            }
            weather_records.append(record)
        
        return pd.DataFrame(weather_records)

    def _process_air_quality_data(self, data):
        """Transform air quality data into a structured format"""
        air_quality_records = []
        
        for city_data in data:
            air = city_data['air_quality']['list'][0]['components']
            record = {
                'city': city_data['city'],
                'timestamp': pd.to_datetime(city_data['timestamp']),
                'co': air['co'],
                'no': air['no'],
                'no2': air['no2'],
                'o3': air['o3'],
                'so2': air['so2'],
                'pm2_5': air['pm2_5'],
                'pm10': air['pm10'],
                'nh3': air['nh3']
            }
            air_quality_records.append(record)
        
        return pd.DataFrame(air_quality_records)

    def calculate_weather_metrics(self, weather_df):
        """Calculate additional weather metrics"""
        # Calculate day length in hours
        weather_df['day_length'] = (weather_df['sunset'] - weather_df['sunrise']).dt.total_seconds() / 3600
        
        # Create temperature category
        weather_df['temp_category'] = pd.cut(
            weather_df['temperature'],
            bins=[-float('inf'), 0, 10, 20, 30, float('inf')],
            labels=['Freezing', 'Cold', 'Mild', 'Warm', 'Hot']
        )
        
        # Calculate heat index (simplified version)
        weather_df['heat_index'] = weather_df.apply(
            lambda x: x['temperature'] + 0.5555 * (6.11 * np.exp(5417.7530 * (1/273.16 - 1/(273.15 + x['temperature']))) * x['humidity']/100 - 10),
            axis=1
        )
        
        return weather_df

    def calculate_air_quality_index(self, air_df):
        """Calculate overall air quality index (simplified version)"""
        # Normalize each pollutant to a 0-100 scale and take the max
        pollutants = ['pm2_5', 'pm10', 'no2', 'o3', 'co', 'so2']
        
        # Reference values (simplified)
        max_values = {
            'pm2_5': 250,
            'pm10': 430,
            'no2': 400,
            'o3': 240,
            'co': 50,
            'so2': 350
        }
        
        for pollutant in pollutants:
            air_df[f'{pollutant}_index'] = (air_df[pollutant] / max_values[pollutant] * 100).clip(0, 100)
        
        air_df['aqi'] = air_df[[f'{p}_index' for p in pollutants]].max(axis=1)
        
        # Add AQI category
        air_df['aqi_category'] = pd.cut(
            air_df['aqi'],
            bins=[0, 20, 40, 60, 80, 100],
            labels=['Very Good', 'Good', 'Moderate', 'Poor', 'Very Poor']
        )
        
        return air_df

    def transform_and_save(self):
        """Transform the latest weather data and save processed results"""
        input_file = self.get_latest_data_file()
        if not input_file:
            print("No weather data files found")
            return None
        
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        # Transform weather data
        weather_df = self._process_weather_data(data)
        weather_df = self.calculate_weather_metrics(weather_df)
        
        # Transform air quality data
        air_quality_df = self._process_air_quality_data(data)
        air_quality_df = self.calculate_air_quality_index(air_quality_df)
        
        # Save processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        weather_output = os.path.join(self.output_dir, f'processed_weather_{timestamp}.csv')
        air_output = os.path.join(self.output_dir, f'processed_air_{timestamp}.csv')
        
        weather_df.to_csv(weather_output, index=False)
        air_quality_df.to_csv(air_output, index=False)
        
        return weather_output, air_output

if __name__ == "__main__":
    transformer = WeatherTransformer()
    transformer.transform_and_save() 