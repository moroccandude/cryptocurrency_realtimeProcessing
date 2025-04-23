FROM python:latest

RUN mkdir -p /app/src
WORKDIR /app

COPY consumer.py ./src/consumer.py
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python3", "./src/consumer.py"]
