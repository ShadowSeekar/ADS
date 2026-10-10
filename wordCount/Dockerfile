FROM python:3.9-slim-bookworm
RUN apt-get update
#RUN apt-get install-y "any package you like"
RUN pip install rpyc redis matplotlib
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]