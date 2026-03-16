FROM python:3.10-slim

WORKDIR /app

COPY damage_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY damage_service/ ./damage_service/
COPY shared/ ./shared/

ENV PYTHONPATH=/app

CMD ["python", "-m", "damage_service.main"]
