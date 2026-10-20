FROM python:3.11-slim
RUN apt-get update
WORKDIR /app
RUN pip install rpyc redis matplotlib
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]