FROM python:3.11-slim-bookworm

# Install OpenJDK 17 (headless) and procps required by PySpark 3.5
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Pre-warm Delta Lake Spark JARs into ~/.ivy2 during build so runtime starts instantly
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
    s = configure_spark_with_delta_pip(SparkSession.builder.master('local[1]').appName('jar-warmup')).getOrCreate(); \
    s.range(1).count(); s.stop()"

# Copy project source and configuration
COPY . /app

# Default entrypoint runs the full 7-step Medallion pipeline and verification report
CMD ["python", "-m", "src.cli", "run-all"]
