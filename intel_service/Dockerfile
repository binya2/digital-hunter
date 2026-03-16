FROM python:3.10-slim

WORKDIR /app

COPY intel_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY intel_service/ ./intel_service/
COPY shared/ ./shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "intel_service.main"]
