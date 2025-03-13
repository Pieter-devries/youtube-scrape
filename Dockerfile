FROM python:3.9-slim-buster

WORKDIR /app

COPY main.py .
COPY requirements.txt .

# Install dependencies into this container so there's no need to 
# install anything at container run time.
RUN pip install -r requirements.txt

# Service must listen to $PORT environment variable.
# This default value facilitates local development.
ENV PORT 8080

# Run the web service on container startup. Here you use the gunicorn
# server, with one worker process and 8 threads. For environments 
# with multiple CPU cores, increase the number of workers to match 
# the number of cores available.
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 main:app
