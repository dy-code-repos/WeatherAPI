import os

bind = f"0.0.0.0:{os.getenv('PORT', '8081')}"
workers = int(os.getenv('WORKERS', '4'))
worker_class = "sync"
timeout = 120
keepalive = 5
max_requests = 1000
max_requests_jitter = 50
accesslog = "-"
errorlog = "-"
loglevel = os.getenv('LOG_LEVEL', 'info')



