# Corteva Weather API

A REST API for accessing weather and crop yield data, built with Flask and PostgreSQL.
## Data Modeling
- To store weather data per station

```
CREATE TABLE IF NOT EXISTS weather_data (
    record_date DATE NOT NULL,
    max_temp NUMERIC,
    min_temp NUMERIC,
    precipitation NUMERIC,
    weather_station CHAR(11) NOT NULL,
    PRIMARY KEY (record_date, weather_station
    )
    
```
- To store yearly weather stats

```
 CREATE TABLE IF NOT EXISTS weather_stats (
    weather_station CHAR(11) NOT NULL,
    record_year SMALLINT   NOT NULL,
    avg_min_temp NUMERIC,
    avg_max_temp NUMERIC,
    avg_precipitation NUMERIC,
    PRIMARY KEY (record_year, weather_station)
    ); 
```



## Features

- Weather data ingestion from text files
- Annual weather statistics calculation
- RESTful API with filtering and pagination
- Swagger/OpenAPI documentation
- Docker containerization
- Production-ready with Gunicorn

## Prerequisites

- Docker and Docker Compose (for containerized deployment)
- OR Python 3.9+ and PostgreSQL (for local development)

## Quick Start with Docker 

1. **Clone the repository**
   ``` 
   git clone <repository-url>
   cd WeatherAPI
2. **Start services**
   ```
   docker-compose up -d
4. **Access the API**
   - API: http://localhost:8081
   - Swagger Docs: http://localhost:8081/api/docs
   - Health Check: http://localhost:8081/health

## Local Development

1. **Install dependencies**
   pip install -r requirements.txt
   2. **Configure database**
   - replace values in `src/config.ini` with your database credentials

2. **Run server**h
   python src/server.py
  
## API Endpoints

### GET /api/weather
Retrieve weather data with optional filtering.

**Query Parameters:**
- `station_id` (optional): Filter by weather station ID
- `date` (optional): Filter by date (YYYY-MM-DD)
- `offset` (optional): Page number (default: 1)
- `limit` (optional): Records per page (default: 1000)

**Example:**
curl "http://localhost:8081/api/weather?station_id=USC00110072&limit=10"
### GET /api/weather/stats
Retrieve annual weather statistics.

**Query Parameters:**
- `station_id` (optional): Filter by weather station ID
- `year` (optional): Filter by year
- `offset` (optional): Page number (default: 1)
- `limit` (optional): Records per page (default: 500)

**Example:**
curl "http://localhost:8081/api/weather/stats?year=2010&limit=10"

### GET /api/yield
Retrieve crop yield data.

**Query Parameters:**
- `year` (optional): Filter by year
- `offset` (optional): Page number (default: 1)
- `limit` (optional): Records per page (default: 5)

### GET /health
Health check endpoint.

## Testing

Run unit tests:

python -m pytest src/test_api.py

OR

python src/test_api.py## Project Structure

## Deplyment
### For Deployment Answers : see the [Answer README](answers/README.md)
