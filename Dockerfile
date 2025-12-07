# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# System deps (optional, minimal here)
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project code
COPY . .

# run consumer.py by default
CMD ["python", "consumer.py"]
