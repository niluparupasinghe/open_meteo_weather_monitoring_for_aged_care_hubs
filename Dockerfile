FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install Python dependencies (native Rust delta-rs + DuckDB + PyArrow + Streamlit)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project source and configuration
COPY . .

# Ensure runtime directories exist
RUN mkdir -p /app/data/landing/raw_json \
             /app/data/lakehouse/ops \
             /app/data/lakehouse/bronze \
             /app/data/lakehouse/silver \
             /app/data/lakehouse/gold \
             /app/logs

CMD ["python", "-m", "src.cli", "run-all"]
