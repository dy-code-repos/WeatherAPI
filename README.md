# Corteva Weather API

A REST API for accessing weather and crop yield data, built with Flask and PostgreSQL.

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
   git clone <repository-url>
   cd code-challenge-template
   2. **Start services**
   docker-compose up -d
   3. **Initialize database and load data**
   # Initialize tables
   docker-compose exec api python -c "from src.data_modeling import get_db_connection, initialize_tables; conn = get_db_connection(); initialize_tables(conn); conn.close()"
   
   # Load weather data
   docker-compose exec api python src/data_wrangling.py
   
   # Calculate statistics
   docker-compose exec api python -c "from src.data_modeling import get_db_connection, calculate_weather_stats; conn = get_db_connection(); calculate_weather_stats(conn); conn.close()"
   4. **Access the API**
   - API: http://localhost:8081
   - Swagger Docs: http://localhost:8081/api/docs
   - Health Check: http://localhost:8081/health

## Local Development

1. **Install dependencies**
   pip install -r requirements.txt
   2. **Configure database**
   - Copy `.env.example` to `.env` and update values
   - OR create `src/config.ini` with database credentials

3. **Initialize database**
   
   from src.data_modeling import get_db_connection, initialize_tables
   conn = get_db_connection()
   initialize_tables(conn)
   conn.close()
   4. **Load data**
   
   python src/data_wrangling.py
   5. **Calculate statistics**thon
   from src.data_modeling import get_db_connection, calculate_weather_stats
   conn = get_db_connection()
   calculate_weather_stats(conn)
   conn.close()
   6. **Run server**h
   python src/server.py
   # OR with gunicorn for production
   gunicorn -c gunicorn_config.py src.server:app
   ## API Endpoints

### GET /api/weather
Retrieve weather data with optional filtering.

**Query Parameters:**
- `station_id` (optional): Filter by weather station ID
- `date` (optional): Filter by date (YYYY-MM-DD)
- `offset` (optional): Page number (default: 1)
- `limit` (optional): Records per page (default: 1000)

**Example:**
curl "http://localhost:8081/api/weather?station_id=USC00110072&limit=10"### GET /api/weather/stats
Retrieve annual weather statistics.

**Query Parameters:**
- `station_id` (optional): Filter by weather station ID
- `year` (optional): Filter by year
- `offset` (optional): Page number (default: 1)
- `limit` (optional): Records per page (default: 500)

**Example:**
curl "http://localhost:8081/api/weather/stats?year=2010&limit=10"### GET /api/yield
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
# OR
python src/test_api.py## Project Structure