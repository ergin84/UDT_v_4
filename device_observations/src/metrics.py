from prometheus_client import Counter, start_http_server

MESSAGES_SENT = Counter('messages_sent', 'Number of messages sent')
MESSAGES_RECEIVED = Counter('messages_received', 'Number of messages received')

def start_metrics_server():
    start_http_server(8000)