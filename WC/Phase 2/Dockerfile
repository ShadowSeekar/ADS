FROM python:3.11-slim
WORKDIR /app
RUN apt-get update
#RUN apt-get install-y "any package you like"
RUN pip install rpyc redis matplotlib
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]

# Compose overrides this per service (server runs server.py, client sleeps).
#CMD ["sleep", "infinity"]