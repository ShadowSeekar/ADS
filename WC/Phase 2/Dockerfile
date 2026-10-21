FROM python:3.11-slim
WORKDIR /app
RUN apt-get update
RUN pip install rpyc redis matplotlib
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]