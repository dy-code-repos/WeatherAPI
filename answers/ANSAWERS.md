# Answers and Discussion
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
- Logging table for ETL weather ingestion runs

```
    CREATE TABLE IF NOT EXISTS weather_logs (
        start_time        TIMESTAMP    NOT NULL,
        end_time          TIMESTAMP    NOT NULL,
        records           INTEGER      NOT NULL,
        weather_station   CHAR(11)     NOT NULL
    );
```
           
- Annual crop yield information

```
    CREATE TABLE IF NOT EXISTS yield_data (
        record_year   SMALLINT    NOT NULL,
        total_yield   INTEGER     NOT NULL,
        PRIMARY KEY (record_year)
    );
```

## Ingestion and Analysis
- Converted the missing values -9999 to Null
- Created PRIMARY KEY (record_year, weather_station) to avoid duplicate insertion 
- Insert into weather_logs for logging and auditing
- Once the weather data is inserted, weather_stats will be updated with yearly data

## REST API
- Used Flask to create REST APIs
- First, it will create all tables and insert the date
- Note: Since it is inserting data first, it might take a few seconds to start serving the API 


