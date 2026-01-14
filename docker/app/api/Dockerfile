FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Expose FastAPI port
EXPOSE 8000

# Set environment (can be overridden at runtime)
ENV ENV=production

CMD ["python", "src"]