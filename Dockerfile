# Dockerfile
FROM python:3.11-slim

# Install git
RUN apt-get update && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# System deps (optional, minimal here)
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project code
COPY . .

RUN chmod +x /app/auto_git_push.sh

# run consumer.py by default
CMD ["python", "consumer.py"]
