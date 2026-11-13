FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka

COPY app.py .

CMD ["python", "app.py"]