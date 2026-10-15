# Gunakan base image Python yang stabil dan ringan
FROM python:3.11-slim

# Set environment variables agar Python tidak men-buffer log dan tidak membuat .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000

# Install dependensi sistem dasar yang dibutuhkan (gcc, curl untuk healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Tentukan direktori kerja di dalam container
WORKDIR /app

# Copy requirements terlebih dahulu untuk caching layer Docker yang efisien
COPY requirements.txt .

# Install dependensi Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy seluruh source code bot ke dalam container
COPY . .

# Buat folder data lokal jika belum ada untuk SQLite
RUN mkdir -p /app/data

# Buka port 8000 untuk HTTP health check K3s Helipod
EXPOSE 8000

# Jalankan uvicorn yang membungkus main:app (FastAPI + Worker Supervisor)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
