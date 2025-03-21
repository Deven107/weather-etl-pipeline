# Weather and Air Quality ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline that collects and processes weather and air quality data from OpenWeather API using Apache Airflow. The pipeline tracks weather conditions and air quality metrics for major cities worldwide, providing historical data and daily statistics.

## Features

- Extracts weather and air quality data from OpenWeather API
- Tracks multiple major cities worldwide
- Calculates advanced weather metrics (heat index, day length)
- Computes air quality indices and categories
- Maintains historical data in SQLite database
- Generates daily statistics and trends
- Automated hourly updates using Apache Airflow

## Project Structure
```
airflow_etl_project/
├── dags/                  # Airflow DAG files
│   └── weather_etl_dag.py
├── data/
│   ├── raw/              # Raw JSON files from OpenWeather API
│   ├── processed/        # Transformed CSV files
│   └── sample_data/      # Sample data files for reference
├── scripts/              # ETL scripts
│   ├── weather_extractor.py
│   ├── weather_transformer.py
│   └── weather_loader.py
├── logs/                 # Airflow logs
└── requirements.txt      # Project dependencies
```

## Prerequisites

1. Python 3.8+
2. OpenWeather API Key (Free Tier Available)
3. Apache Airflow

## OpenWeather API Free Tier

This project is compatible with OpenWeather's Free API tier, which includes:
- 60 calls/minute for current weather data
- 60 calls/minute for air pollution data
- No credit card required
- Sufficient for personal projects and testing

To get your free API key:
1. Sign up at https://openweathermap.org/api
2. Verify your email
3. Your API key will be available in your account
4. API key activation may take up to 2 hours

## Sample Data

The project includes sample data files in the `data/sample_data` directory that demonstrate the structure of:
- Raw API responses
- Transformed data
- Database schema

This allows you to understand the data flow without needing an API key.

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/weather-etl-pipeline.git
cd weather-etl-pipeline
```

2. Create a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your OpenWeather API key:
```
OPENWEATHER_API_KEY=your_api_key_here
```

5. Set up Airflow:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

6. Start Airflow:
```bash
airflow webserver -p 8080
airflow scheduler
```

7. Access the Airflow UI at http://localhost:8080

## Running Without API Key

To explore the project without an API key:
1. Check the sample data files in `data/sample_data`
2. Review the transformation logic in `scripts/weather_transformer.py`
3. Examine the database schema in `scripts/weather_loader.py`

## Pipeline Description

The ETL pipeline consists of three main tasks:

1. **Extract (weather_extractor.py)**:
   - Connects to OpenWeather API
   - Retrieves current weather conditions
   - Fetches air quality data
   - Saves raw data as JSON files

2. **Transform (weather_transformer.py)**:
   - Processes raw weather and air quality data
   - Calculates additional metrics:
     - Heat index
     - Day length
     - Temperature categories
     - Air Quality Index (AQI)
   - Creates normalized CSV files

3. **Load (weather_loader.py)**:
   - Maintains SQLite database schema
   - Loads processed data into tables
   - Generates daily statistics
   - Tracks historical trends

The pipeline runs hourly to maintain up-to-date weather and air quality information.

## Database Schema

The SQLite database contains three main tables:

1. **weather_measurements**:
   - Current weather conditions
   - Temperature and humidity
   - Wind and cloud data
   - Sunrise/sunset times
   - Calculated metrics (heat index, day length)

2. **air_quality_measurements**:
   - Air pollutant levels (CO, NO2, SO2, etc.)
   - Particulate matter (PM2.5, PM10)
   - Air Quality Index
   - Quality categories

3. **city_daily_stats**:
   - Daily aggregated statistics
   - Average/max/min temperatures
   - Average humidity and AQI
   - Dominant weather conditions

## Resume Points

This project demonstrates:
- ETL pipeline development with Python
- RESTful API integration (OpenWeather API)
- Data transformation and analysis with Pandas
- Database design and SQL
- Workflow orchestration with Apache Airflow
- Environmental data processing
- Time series data handling
- Statistical computations
- Production-grade code organization

## Monitored Cities

The pipeline tracks weather and air quality for major global cities including:
- New York, USA
- London, UK
- Tokyo, Japan
- Sydney, Australia
- Paris, France
- Mumbai, India
- Dubai, UAE
- Singapore
- San Francisco, USA
- Toronto, Canada

More cities can be easily added by modifying the configuration in `weather_extractor.py`.

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 