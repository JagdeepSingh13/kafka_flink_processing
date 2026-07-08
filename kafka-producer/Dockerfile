FROM python:3.8-slim

WORKDIR /

COPY requirements.txt python-producer.py wait-for-it.sh ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python -u python-producer.py