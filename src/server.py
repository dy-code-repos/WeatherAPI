import os
from flask import Flask, request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
import data_modeling
import data_wrangling

def create_app():
    """
    Create and configure the Flask application with all API endpoints.
    
    Returns:
        Flask app instance
    """
    app = Flask(__name__)

    try:
        conn = data_modeling.get_db_connection()
        data_modeling.initialize_tables(conn)
        
        # Get the directory where this file is located (src directory)
        src_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(src_dir)
        
        # Ingest weather data
        wx_data_path = os.path.join(src_dir, "wx_data")
        weather_success = data_wrangling.ingest_weather_data(wx_data_path)
        if weather_success:
            print("Weather data ingestion completed successfully.")
        else:
            print("Weather data ingestion failed.")
        
        # Ingest yield data
        yld_data_path = os.path.join(src_dir, "yld_data", "US_corn_grain_yield.txt")
        yield_success = data_wrangling.ingest_yield_data(yld_data_path)
        if yield_success:
            print("Yield data ingestion completed successfully.")
        else:
            print("Yield data ingestion failed.")
        
        # Calculate weather stats
        if weather_success:
            data_modeling.calculate_weather_stats(conn)
            conn.close()
    except Exception as e:
        print(f"Error: {e}")
    
    # Configure Flask based on environment
    app.config['DEBUG'] = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Swagger/OpenAPI configuration
    SWAGGER_URL = '/api/docs'
    API_URL = '/static/swagger.json'
    
    try:
        swaggerui_blueprint = get_swaggerui_blueprint(
            SWAGGER_URL,
            API_URL,
            config={
                'app_name': "Corteva Weather API"
            }
        )
        app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)
    except Exception as e:
        # Swagger is optional, continue without it if it fails
        print(f"Warning: Swagger UI not available: {e}")
    

    @app.route('/api/weather', methods=['GET'])
    def fetch_weather_data():
        """
        GET /api/weather
        Retrieve weather data with optional filtering and pagination.
        
        Query Parameters:
            station_id (str, optional): Filter by weather station ID
            date (str, optional): Filter by date in YYYY-MM-DD format
            offset (int, optional): Page number (1-indexed, default: 1)
            limit (int, optional): Records per page (default: 1000)
        
        Returns:
            JSON array of weather records with temperatures in Celsius and precipitation in cm
        """
        try:
            args = request.args
            station_id = args.get("station_id", "", type=str)
            date_val = args.get("date", "", type=str)
            offset = args.get("offset", 1, type=int)
            limit = args.get("limit", 1000, type=int)

            # Validate inputs
            if offset < 1:
                return jsonify({"error": "offset must be >= 1"}), 400
            if limit < 1 or limit > 10000:
                return jsonify({"error": "limit must be between 1 and 10000"}), 400

            records = data_modeling.get_weather_data(station_id, date_val, offset, limit)
            return jsonify({
                "data": records,
                "count": len(records),
                "offset": offset,
                "limit": limit
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/api/yield', methods=['GET'])
    def fetch_yield_data():
        """
        GET /api/yield
        Retrieve yield data with optional filtering and pagination.
        
        Query Parameters:
            year (int, optional): Filter by year
            offset (int, optional): Page number (1-indexed, default: 1)
            limit (int, optional): Records per page (default: 5)
        
        Returns:
            JSON array of yield records
        """
        try:
            args = request.args
            year_val = args.get("year", 0, type=int)
            offset = args.get("offset", 1, type=int)
            limit = args.get("limit", 5, type=int)

            # Validate inputs
            if offset < 1:
                return jsonify({"error": "offset must be >= 1"}), 400
            if limit < 1 or limit > 1000:
                return jsonify({"error": "limit must be between 1 and 1000"}), 400

            records = data_modeling.get_yield_data(year_val, offset, limit)
            return jsonify({
                "data": records,
                "count": len(records),
                "offset": offset,
                "limit": limit
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/api/weather/stats', methods=['GET'])
    def fetch_weather_stats():
        """
        GET /api/weather/stats
        Retrieve weather statistics with optional filtering and pagination.
        
        Query Parameters:
            station_id (str, optional): Filter by weather station ID
            year (int, optional): Filter by year
            offset (int, optional): Page number (1-indexed, default: 1)
            limit (int, optional): Records per page (default: 500)
        
        Returns:
            JSON array of weather statistics with temperatures in Celsius and precipitation in cm
        """
        try:
            args = request.args
            station_id = args.get("station_id", "", type=str)
            year_val = args.get("year", 0, type=int)
            offset = args.get("offset", 1, type=int)
            limit = args.get("limit", 500, type=int)

            # Validate inputs
            if offset < 1:
                return jsonify({"error": "offset must be >= 1"}), 400
            if limit < 1 or limit > 1000:
                return jsonify({"error": "limit must be between 1 and 1000"}), 400

            records = data_modeling.get_weather_stats(station_id, year_val, offset, limit)
            return jsonify({
                "data": records,
                "count": len(records),
                "offset": offset,
                "limit": limit
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({"status": "healthy"}), 200

    return app


# Create app instance for gunicorn
app = create_app()

if __name__ == "__main__":
    port = int(os.getenv('PORT', 8081))
    app.run(host='0.0.0.0', port=port, debug=app.config['DEBUG'])
