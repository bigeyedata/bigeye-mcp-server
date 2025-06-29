# Use Python 3.12 slim image for smaller size
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt* ./

# Install Python dependencies
# If requirements.txt doesn't exist, install directly
RUN if [ -f requirements.txt ]; then \
        pip install --no-cache-dir -r requirements.txt; \
    else \
        pip install --no-cache-dir mcp[cli] httpx; \
    fi

# Copy the application code
COPY . .

# Create a non-root user to run the application
RUN useradd -m -u 1000 mcp && chown -R mcp:mcp /app

# Create directory for credentials (will be mounted)
RUN mkdir -p /home/mcp/.bigeye-mcp && chown -R mcp:mcp /home/mcp/.bigeye-mcp

# Switch to non-root user
USER mcp

# Set HOME environment variable
ENV HOME=/home/mcp

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# The MCP server runs via stdio, so we use the server directly
CMD ["python", "server.py"]