import logging
import json
import sys

class CloudRunFormatter(logging.Formatter):
    def format(self, record):
        # Apply the standard formatting first
        message = super().format(record)
        
        log_dict = {
            'severity': record.levelname,
            'message': message,
            'timestamp': self.formatTime(record, self.datefmt),
            'logger': record.name,
        }
        
        # Use ensure_ascii=False to preserve UTF-8 characters like emojis
        return json.dumps(log_dict, ensure_ascii=False)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Create console handler with CloudRunFormatter
handler = logging.StreamHandler(sys.stdout)  # Cloud Run reads from stdout
formatter = CloudRunFormatter('%(message)s')  # Simple format focusing on the message
handler.setFormatter(formatter)

# Remove existing handlers if any
for h in root_logger.handlers:
    root_logger.removeHandler(h)
root_logger.addHandler(handler)

# Create a logger with name
logger = logging.getLogger('investflow')
