import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

class WeatherLoader:
    def __init__(self, db_path='../data/weather_data.db'):
        self.engine = create_engine(f'sqlite:///{db_path}')
        self.create_tables()

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        with self.engine.connect() as conn:
            # Create weather measurements table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS weather_measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    latitude REAL,
                    longitude REAL,
                    timestamp TIMESTAMP,
                    temperature REAL,
                    feels_like REAL,
                    humidity INTEGER,
                    pressure INTEGER,
                    wind_speed REAL,
                    wind_direction REAL,
                    clouds_percent INTEGER,
                    weather_main TEXT,
                    weather_description TEXT,
                    sunrise TIMESTAMP,
                    sunset TIMESTAMP,
                    day_length REAL,
                    temp_category TEXT,
                    heat_index REAL
                )
            """))
            
            # Create air quality measurements table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS air_quality_measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    timestamp TIMESTAMP,
                    co REAL,
                    no REAL,
                    no2 REAL,
                    o3 REAL,
                    so2 REAL,
                    pm2_5 REAL,
                    pm10 REAL,
                    nh3 REAL,
                    aqi REAL,
                    aqi_category TEXT
                )
            """))
            
            # Create city stats table for aggregated data
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS city_daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    date DATE,
                    avg_temperature REAL,
                    max_temperature REAL,
                    min_temperature REAL,
                    avg_humidity REAL,
                    avg_aqi REAL,
                    dominant_weather TEXT,
                    measurements_count INTEGER
                )
            """))
            
            conn.commit()

    def load_weather_data(self, weather_file):
        """Load processed weather data into the database"""
        df = pd.read_csv(weather_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['sunrise'] = pd.to_datetime(df['sunrise'])
        df['sunset'] = pd.to_datetime(df['sunset'])
        
        df.to_sql('weather_measurements', self.engine, if_exists='append', index=False)

    def load_air_quality_data(self, air_quality_file):
        """Load processed air quality data into the database"""
        df = pd.read_csv(air_quality_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df.to_sql('air_quality_measurements', self.engine, if_exists='append', index=False)

    def update_daily_stats(self):
        """Update the daily statistics for each city"""
        with self.engine.connect() as conn:
            # Calculate daily statistics
            conn.execute(text("""
                INSERT INTO city_daily_stats (
                    city, date, avg_temperature, max_temperature, min_temperature,
                    avg_humidity, avg_aqi, dominant_weather, measurements_count
                )
                SELECT 
                    w.city,
                    date(w.timestamp) as date,
                    avg(w.temperature) as avg_temperature,
                    max(w.temperature) as max_temperature,
                    min(w.temperature) as min_temperature,
                    avg(w.humidity) as avg_humidity,
                    avg(a.aqi) as avg_aqi,
                    w.weather_main as dominant_weather,
                    count(*) as measurements_count
                FROM weather_measurements w
                LEFT JOIN air_quality_measurements a 
                    ON w.city = a.city 
                    AND date(w.timestamp) = date(a.timestamp)
                WHERE date(w.timestamp) = date('now')
                GROUP BY w.city, date(w.timestamp)
                ON CONFLICT (city, date) DO UPDATE SET
                    avg_temperature = excluded.avg_temperature,
                    max_temperature = excluded.max_temperature,
                    min_temperature = excluded.min_temperature,
                    avg_humidity = excluded.avg_humidity,
                    avg_aqi = excluded.avg_aqi,
                    dominant_weather = excluded.dominant_weather,
                    measurements_count = excluded.measurements_count
            """))
            
            conn.commit()

    def load_data(self, input_dir='../data/processed'):
        """Load all processed data into the database"""
        # Find the latest weather and air quality files
        weather_files = [f for f in os.listdir(input_dir) if f.startswith('processed_weather_')]
        air_files = [f for f in os.listdir(input_dir) if f.startswith('processed_air_')]
        
        if not weather_files or not air_files:
            print("No processed data files found")
            return
        
        latest_weather = os.path.join(input_dir, sorted(weather_files)[-1])
        latest_air = os.path.join(input_dir, sorted(air_files)[-1])
        
        # Load the data
        self.load_weather_data(latest_weather)
        self.load_air_quality_data(latest_air)
        
        # Update daily statistics
        self.update_daily_stats()

if __name__ == "__main__":
    loader = WeatherLoader()
    loader.load_data() 