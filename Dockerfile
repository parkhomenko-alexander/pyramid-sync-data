FROM python:3.11.5-slim-bullseye
RUN apt update && apt -y upgrade
WORKDIR /usr/src/app
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app"
COPY ./requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY ./ ./