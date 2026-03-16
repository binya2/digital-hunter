FROM python:3.10-slim

WORKDIR /app

COPY attack_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY attack_service/ ./attack_service/
COPY shared/ ./shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "attack_service.main"]
