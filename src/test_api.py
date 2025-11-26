import unittest
from server import create_app
import data_modeling


class TestWeatherAPI(unittest.TestCase):
    """Unit tests for the Weather API."""

    def setUp(self):
        """Set up test client."""
        self.app = create_app()
        self.client = self.app.test_client()

    def test_health_endpoint(self):
        """Test health check endpoint."""
        response = self.client.get('/health')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['status'], 'healthy')

    def test_weather_endpoint(self):
        """Test weather data endpoint."""
        response = self.client.get('/api/weather?limit=10')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('data', data)
        self.assertIn('count', data)

    def test_weather_stats_endpoint(self):
        """Test weather stats endpoint."""
        response = self.client.get('/api/weather/stats?limit=10')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('data', data)

    def test_yield_endpoint(self):
        """Test yield data endpoint."""
        response = self.client.get('/api/yield?limit=5')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn('data', data)


if __name__ == '__main__':
    unittest.main()
