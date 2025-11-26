import os
import time
import pandas as pd
import psycopg2
import configparser
import psycopg2.extras as extras
from psycopg2 import sql
from psycopg2.extensions import register_adapter, AsIs
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_connection():
    """
    Create and return a database connection using config.ini or environment variables.
    Environment variables take precedence over config.ini.
    
    Returns:
        psycopg2.connection: Database connection object
    """
    # Try environment variables first (for Docker/production)
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')
    
    # Fall back to config.ini if env vars not set (for local development)
    if not all([db_host, db_user, db_password, db_name]):
        parser = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        if os.path.exists(config_path):
            parser.read(config_path)
            db_cfg = parser["postgresqlDB"]
            db_host = db_host or db_cfg.get("host")
            db_user = db_user or db_cfg.get("user")
            db_password = db_password or db_cfg.get("pass")
            db_name = db_name or db_cfg.get("db")
            db_port = db_port or db_cfg.get("port")
    
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Database configuration not found. Set environment variables or config.ini")
    
    connection = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        dbname=db_name,
        port=db_port
    )

    return connection


def initialize_tables(conn):
    """
    Create all required tables if they do not already exist.
    Uses PostgreSQL advisory locks to prevent race conditions when multiple
    workers try to initialize tables concurrently.
    Note: This function does NOT drop existing tables to preserve data.
    For a fresh start, manually drop tables or use initialize_tables_fresh().
    
    Args:
        conn: psycopg2 connection object
    """
    # Use advisory lock to ensure only one process initializes tables at a time
    # Lock ID: 123456 (arbitrary but consistent)
    lock_id = 123456
    
    with conn.cursor() as cur:
        # Try to acquire advisory lock (non-blocking)
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        lock_acquired = cur.fetchone()[0]
        
        if not lock_acquired:
            # Another process is initializing, wait a bit and check if tables exist
            time.sleep(0.5)
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'weather_data'
                )
            """)
            table_exists = cur.fetchone()[0]
            if table_exists:
                print("Tables already initialized by another process, skipping...")
                return
            # If tables don't exist, wait for lock (blocking)
            cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
            lock_acquired = True
        
        try:
            print("creating tables -----------------")
            # Table for raw daily weather observations
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_data (
                    record_date        DATE        NOT NULL,
                    max_temp           NUMERIC,
                    min_temp           NUMERIC,
                    precipitation      NUMERIC,
                    weather_station    CHAR(11)    NOT NULL,
                    PRIMARY KEY (record_date, weather_station)
                );
                """
            )

            # Annual crop yield information
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS yield_data (
                    record_year   SMALLINT    NOT NULL,
                    total_yield   INTEGER     NOT NULL,
                    PRIMARY KEY (record_year)
                );
                """
            )

            # Logging table for ETL weather ingestion runs
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_logs (
                    start_time        TIMESTAMP    NOT NULL,
                    end_time          TIMESTAMP    NOT NULL,
                    records           INTEGER      NOT NULL,
                    weather_station   CHAR(11)     NOT NULL
                );
                """
            )

            # Aggregated annual stats per station
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_stats (
                    weather_station      CHAR(11)   NOT NULL,
                    record_year          SMALLINT   NOT NULL,
                    avg_min_temp         NUMERIC,
                    avg_max_temp         NUMERIC,
                    avg_precipitation    NUMERIC,
                    PRIMARY KEY (record_year, weather_station)
                );
                """
            )

            conn.commit()
        except psycopg2.Error as e:
            # If error is about duplicate type/table, it's likely a race condition
            # that was resolved by another process - this is acceptable
            if "duplicate key value violates unique constraint" in str(e) or \
               "already exists" in str(e).lower():
                print(f"Table initialization conflict (likely resolved by another process): {e}")
                conn.rollback()
            else:
                # Re-raise other errors
                conn.rollback()
                raise
        finally:
            # Release the advisory lock
            if lock_acquired:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                conn.commit()


def get_weather_data(station_id: str = "", date_val: str = "", offset: int = 1, limit: int = 1000) -> List[Dict]:
    """
    Fetch weather data from the database with optional filters and pagination.
    Temperatures are returned in degrees Celsius (converted from tenths).
    Precipitation is returned in centimeters (converted from tenths of mm).

    Args:
        station_id (str): Filter by weather station ID
        date_val (str): Filter by specific date (YYYY-MM-DD)
        offset (int): Page number (1-indexed)
        limit (int): Number of records per page

    Returns:
        list of dict: List of weather records with converted units
    """
    records = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = sql.SQL(
                "SELECT record_date, "
                "max_temp / 10.0 as max_temp, "
                "min_temp / 10.0 as min_temp, "
                "precipitation / 100.0 as precipitation, "
                "weather_station "
                "FROM weather_data WHERE 1=1"
            )

            params = []

            if station_id:
                query += sql.SQL(" AND weather_station = %s")
                params.append(station_id)

            if date_val:
                query += sql.SQL(" AND record_date = %s")
                params.append(date_val)

            # Pagination
            offset_value = (offset - 1) * limit
            query += sql.SQL(" ORDER BY record_date, weather_station LIMIT %s OFFSET %s")
            params.extend([limit, offset_value])

            cur.execute(query, params)
            rows = cur.fetchall()

            # Convert to list of dicts
            columns = [desc[0] for desc in cur.description]
            for row in rows:
                records.append(dict(zip(columns, row)))

    except psycopg2.Error as e:
        print(f"Database error in get_weather_data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    return records


def get_weather_stats(station_id: str = "", year_val: int = 0, offset: int = 1, limit: int = 500) -> List[Dict]:
    """
    Fetch weather statistics from the database with optional filters and pagination.
    Temperatures are returned in degrees Celsius.
    Precipitation is returned in centimeters.

    Args:
        station_id (str): Filter by weather station ID
        year_val (int): Filter by specific year
        offset (int): Page number (1-indexed)
        limit (int): Number of records per page

    Returns:
        list of dict: List of weather statistics records
    """
    records = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = sql.SQL(
                "SELECT weather_station, record_year, "
                "avg_min_temp / 10.0 as avg_min_temp, "
                "avg_max_temp / 10.0 as avg_max_temp, "
                "avg_precipitation / 100.0 as avg_precipitation "
                "FROM weather_stats WHERE 1=1"
            )

            params = []

            if station_id:
                query += sql.SQL(" AND weather_station = %s")
                params.append(station_id)

            if year_val:
                query += sql.SQL(" AND record_year = %s")
                params.append(year_val)

            # Pagination
            offset_value = (offset - 1) * limit
            query += sql.SQL(" ORDER BY record_year, weather_station LIMIT %s OFFSET %s")
            params.extend([limit, offset_value])

            cur.execute(query, params)
            rows = cur.fetchall()

            # Convert to list of dicts
            columns = [desc[0] for desc in cur.description]
            for row in rows:
                records.append(dict(zip(columns, row)))

    except psycopg2.Error as e:
        print(f"Database error in get_weather_stats: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    return records


def get_yield_data(year_val: int = 0, offset: int = 1, limit: int = 5) -> List[Dict]:
    """
    Fetch yield data from the database with optional filters and pagination.

    Args:
        year_val (int): Filter by specific year
        offset (int): Page number (1-indexed)
        limit (int): Number of records per page

    Returns:
        list of dict: List of yield records
    """
    records = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = sql.SQL("SELECT record_year, total_yield FROM yield_data WHERE 1=1")
            params = []

            if year_val:
                query += sql.SQL(" AND record_year = %s")
                params.append(year_val)

            # Pagination
            offset_value = (offset - 1) * limit
            query += sql.SQL(" ORDER BY record_year LIMIT %s OFFSET %s")
            params.extend([limit, offset_value])

            cur.execute(query, params)
            rows = cur.fetchall()

            # Convert to list of dicts
            columns = [desc[0] for desc in cur.description]
            for row in rows:
                records.append(dict(zip(columns, row)))

    except psycopg2.Error as e:
        print(f"Database error in get_yield_data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    return records


def calculate_weather_stats(conn) -> bool:
    """
    Calculate annual weather statistics for each station and year.
    For every year, for every weather station, calculates:
    - Average maximum temperature (in tenths of degrees Celsius)
    - Average minimum temperature (in tenths of degrees Celsius)
    - Total accumulated precipitation (in tenths of millimeters, converted to cm in API)
    
    Missing data (NULL values) are ignored in calculations.
    Results are stored in weather_stats table using UPSERT to handle duplicates.

    Args:
        conn: psycopg2 connection object

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.cursor() as cur:
            # Calculate stats, ignoring NULL values
            query = """
                INSERT INTO weather_stats (weather_station, record_year, avg_max_temp, avg_min_temp, avg_precipitation)
                SELECT 
                    weather_station,
                    EXTRACT(YEAR FROM record_date)::SMALLINT as record_year,
                    AVG(max_temp) as avg_max_temp,
                    AVG(min_temp) as avg_min_temp,
                    SUM(precipitation) as avg_precipitation
                FROM weather_data
                WHERE max_temp IS NOT NULL OR min_temp IS NOT NULL OR precipitation IS NOT NULL
                GROUP BY weather_station, EXTRACT(YEAR FROM record_date)
                ON CONFLICT (record_year, weather_station) 
                DO UPDATE SET
                    avg_max_temp = EXCLUDED.avg_max_temp,
                    avg_min_temp = EXCLUDED.avg_min_temp,
                    avg_precipitation = EXCLUDED.avg_precipitation
            """
            cur.execute(query)
            conn.commit()
            print("Weather statistics calculated and stored successfully.")
            return True
    except psycopg2.Error as e:
        print(f"Error calculating weather stats: {e}")
        conn.rollback()
        return False