import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2 import extras, sql
from data_modeling import get_db_connection


def load_weather_station_data(directory_path: str):
    """
    Reads all weather station .txt files from the specified directory,
    cleans the data, replaces missing values, converts dates, and logs processing.

    Args:
        directory_path (str): Path to folder containing weather station files.

    Returns:
        tuple: (weather_station_data_df, weather_station_logs_df)
    """
    columns = ['record_date', 'max_temp', 'min_temp', 'precipitation', 'weather_station']
    logs = []
    df_list = []

    # Iterate over all txt files in the directory
    for filepath in glob.glob(os.path.join(directory_path, "*.txt")):
        station_name = os.path.splitext(os.path.basename(filepath))[0]
        start_time = datetime.now()
        
        # Read file into DataFrame
        df = pd.read_csv(filepath, header=None, names=columns, delimiter='\t', index_col=False)
        df['weather_station'] = station_name

        # Replace -9999 with NaN
        df.replace(-9999, np.nan, inplace=True)

        # Convert 'record_date' from YYYYMMDD string to datetime
        df['record_date'] = pd.to_datetime(df['record_date'].astype(str), format='%Y%m%d')

        df_list.append(df)

        # Append log info
        end_time = datetime.now()
        logs.append([start_time, end_time, len(df), station_name])

    # Combine all station data into a single DataFrame
    weather_station_data = pd.concat(df_list, ignore_index=True)

    # Create logs DataFrame
    weather_station_logs = pd.DataFrame(
        logs,
        columns=['start_time', 'end_time', 'records', 'weather_station']
    )
    print(weather_station_logs)
    return weather_station_data, weather_station_logs


def insert_dataframe(conn, df: pd.DataFrame, table_name: str, check_duplicates: bool = True) -> bool:
    """
    Inserts a pandas DataFrame into a PostgreSQL table using execute_values for efficiency.
    Handles duplicates by using ON CONFLICT DO NOTHING.

    Args:
        conn (psycopg2 connection): Active PostgreSQL connection.
        df (pd.DataFrame): DataFrame to insert.
        table_name (str): Target table name.
        check_duplicates (bool): If True, skip duplicate records based on primary key.

    Returns:
        bool: True if insert successful, False if error occurs.
    """
    if df.empty:
        print(f"No data to insert into {table_name}.")
        return True

    # Convert DataFrame to list of tuples
    records = list(df.itertuples(index=False, name=None))

    # Build column list - use parameterized query to prevent SQL injection
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))

    # Prepare SQL query with conflict handling
    if check_duplicates and table_name == "weather_data":
        # Use ON CONFLICT for weather_data (has composite primary key)
        query = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES %s "
            "ON CONFLICT (record_date, weather_station) DO NOTHING"
        ).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(columns)
        )
    elif check_duplicates and table_name == "yield_data":
        # Use ON CONFLICT for yield_data (has single primary key)
        query = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES %s "
            "ON CONFLICT (record_year) DO NOTHING"
        ).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(columns)
        )
    else:
        # No duplicate checking
        query = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES %s"
        ).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(columns)
        )

    try:
        with conn.cursor() as cur:
            total_records = len(records)
            
            # Perform the insert
            extras.execute_values(cur, query, records, template=None, page_size=1000)
            rows_inserted = cur.rowcount
            conn.commit()
            
            if check_duplicates:
                skipped_count = total_records - rows_inserted if rows_inserted >= 0 else 0
                if skipped_count > 0:
                    print(f"Data inserted into {table_name}: {rows_inserted} new records inserted, {skipped_count} duplicates skipped (out of {total_records} total).")
                else:
                    print(f"Data inserted into {table_name}: {rows_inserted} records inserted (duplicate checking enabled).")
            else:
                print(f"Data inserted successfully into {table_name}. {total_records} records processed.")
        return True

    except psycopg2.DatabaseError as e:
        print(f"Error inserting data into {table_name}: {e}")
        conn.rollback()
        return False


def ingest_weather_data(directory_path: str) -> bool:
    """
    Complete ingestion pipeline for weather data.
    Loads data, inserts into database with duplicate checking, and saves logs.
    Logs overall start/end times and total number of records ingested.

    Args:
        directory_path (str): Path to folder containing weather station files.

    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    ingestion_start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"Weather Data Ingestion Started: {ingestion_start_time}")
    print(f"{'='*60}")
    
    try:
        conn = get_db_connection()
        
        # Load data
        data, logs = load_weather_station_data(directory_path)
        total_records = len(data)
        print(f"\nTotal records loaded from files: {total_records}")
        
        # Insert weather data with duplicate checking
        success = insert_dataframe(conn, data, "weather_data", check_duplicates=True)
        if not success:
            return False
        
        # Insert logs
        success = insert_dataframe(conn, logs, "weather_logs", check_duplicates=False)
        if not success:
            return False
        
        ingestion_end_time = datetime.now()
        duration = ingestion_end_time - ingestion_start_time
        
        print(f"\n{'='*60}")
        print(f"Weather Data Ingestion Completed: {ingestion_end_time}")
        print(f"Total Duration: {duration}")
        print(f"Total Records Processed: {total_records}")
        print(f"{'='*60}\n")
        
        return True
    except Exception as e:
        ingestion_end_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"Error in weather data ingestion: {e}")
        print(f"Ingestion Failed at: {ingestion_end_time}")
        print(f"{'='*60}\n")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def load_yield_data(file_path: str) -> pd.DataFrame:
    """
    Load yield data from text file.
    
    Args:
        file_path (str): Path to yield data file
        
    Returns:
        pd.DataFrame: DataFrame with yield data
    """
    df = pd.read_csv(file_path, header=None, names=['record_year', 'total_yield'], delimiter='\t')
    return df


def ingest_yield_data(file_path: str) -> bool:
    """
    Ingest yield data from file into database.
    Logs overall start/end times and total number of records ingested.

    Args:
        file_path (str): Path to yield data file

    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    ingestion_start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"Yield Data Ingestion Started: {ingestion_start_time}")
    print(f"{'='*60}")
    
    try:
        conn = get_db_connection()
        df = load_yield_data(file_path)
        total_records = len(df)
        print(f"\nTotal records loaded from file: {total_records}")
        
        success = insert_dataframe(conn, df, "yield_data", check_duplicates=True)
        
        if success:
            ingestion_end_time = datetime.now()
            duration = ingestion_end_time - ingestion_start_time
            
            print(f"\n{'='*60}")
            print(f"Yield Data Ingestion Completed: {ingestion_end_time}")
            print(f"Total Duration: {duration}")
            print(f"Total Records Processed: {total_records}")
            print(f"{'='*60}\n")
        
        return success
    except Exception as e:
        ingestion_end_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"Error in yield data ingestion: {e}")
        print(f"Ingestion Failed at: {ingestion_end_time}")
        print(f"{'='*60}\n")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Ingest weather data
    weather_success = ingest_weather_data("../wx_data")
    if weather_success:
        print("Weather data ingestion completed successfully.")
    else:
        print("Weather data ingestion failed.")
    
    # Ingest yield data
    yield_success = ingest_yield_data("../yld_data/US_corn_grain_yield.txt")
    if yield_success:
        print("Yield data ingestion completed successfully.")
    else:
        print("Yield data ingestion failed.")
    
    # Calculate weather stats
    if weather_success:
        conn = get_db_connection()
        from data_modeling import calculate_weather_stats
        calculate_weather_stats(conn)
        conn.close()