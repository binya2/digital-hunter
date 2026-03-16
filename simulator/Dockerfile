FROM python:3.10-slim

WORKDIR /app

COPY simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator/ ./simulator/

ENV PYTHONPATH=/app

CMD ["python", "-m", "simulator.simulator"]
