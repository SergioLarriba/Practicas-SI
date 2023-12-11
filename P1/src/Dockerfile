#FROM ubuntu
FROM localhost:5000/ubuntu:latest

ENV NUMWORKERS 2

# Switch to root temporarily to install packages
USER root

RUN apt update
RUN apt install -y python3-pip

RUN pip install hypercorn quart

RUN mkdir -p /si1/build/users

EXPOSE 8000

RUN chown -R si1 /si1

USER si1

WORKDIR /si1
COPY user_rest.py ./

CMD hypercorn --bind 0.0.0.0:8000 --workers ${NUMWORKERS} user_rest:app


