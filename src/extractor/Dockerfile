FROM python:latest

RUN mkdir -p /app/src
WORKDIR /app

COPY connector.py ./src/connector.py
COPY requirements.txt .
COPY .env ./src/.env
RUN pip install -r requirements.txt

CMD ["python3", "./src/connector.py"]
