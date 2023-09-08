FROM python:3.9
WORKDIR /usr/src/ingest
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY *.py .
COPY credentials/* credentials
#COPY /home/fgennari/.aws/credentials /root/.aws/credentials
COPY client_secrets.json .
RUN mkdir logs
RUN mkdir /audio_tags/
#ENTRYPOINT ["entrypoint.sh"]
CMD [ "python", "./main.py" ]
