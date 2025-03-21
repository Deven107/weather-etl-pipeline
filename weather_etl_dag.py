from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from weather_extractor import WeatherExtractor
from weather_transformer import WeatherTransformer
from weather_loader import WeatherLoader

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_weather_data():
    """Extract weather and air quality data from OpenWeather API"""
    extractor = WeatherExtractor()
    return extractor.extract_and_save()

def transform_weather_data():
    """Transform raw weather and air quality data"""
    transformer = WeatherTransformer()
    return transformer.transform_and_save()

def load_weather_data():
    """Load transformed data into the database"""
    loader = WeatherLoader()
    loader.load_data()

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather and air quality data from OpenWeather API',
    schedule_interval=timedelta(hours=1),  # Run hourly to track weather changes
    start_date=datetime(2024, 3, 21),
    catchup=False,
    tags=['weather', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
    )
    
    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
    )
    
    # Set task dependencies
    extract_task >> transform_task >> load_task 