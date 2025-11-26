FROM python:3.9-slim

# Set working directory
WORKDIR /app
ENV PYTHONPATH="/app/src"
# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
RUN echo "Listing files:" && ls -R .
COPY wx_data/ ./wx_data/
COPY yld_data/ ./yld_data/

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')"

# Use gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:8081", "--workers", "4", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "src.server:app"]



