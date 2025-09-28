# Minimal image to run the extractor
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY es_pii_extract_update.py ./
COPY detectors.yaml ./

# Optional: copy sample files
COPY pii_test_samples.txt ./

# Default command prints help
CMD ["python", "es_pii_extract_update.py", "--help"]
