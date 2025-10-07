# Use Apache Spark base image
FROM apache/spark:3.5.7-python3

# Run as root user
USER root

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required for cassandra-driver C extensions
RUN apt-get update && \
    apt-get install -y \
        gcc \
        python3-dev \
        libev4 \
        libev-dev \
        build-essential \
        python3-setuptools \

    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# make sure uv is in the path
ENV PATH="/root/.local/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy pyproject.toml and uv.lock (if exists)
COPY pyproject.toml ./
COPY uv.lock* ./

# Install dependencies using uv
RUN uv sync --frozen

COPY . .

# Set the Python path to include the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYSPARK_PYTHON=/app/.venv/bin/python

# Default command (can be overridden)
CMD ["python", "-c", "import pyspark; print('Spark environment ready')"]
