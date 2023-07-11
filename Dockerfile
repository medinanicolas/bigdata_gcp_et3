FROM gcr.io/google.com/cloudsdktool/google-cloud-cli
#RUN gcloud components install app-engine-python --quiet
RUN apt update && apt install python3 python3-dev python3-pip curl build-essential -y
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
ENTRYPOINT ["flask", "--app", "app", "run", "--host", "0.0.0.0"]