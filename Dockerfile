FROM python:3.8-slim

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /reanalysis

COPY . /reanalysis

RUN mkdir -p output/ logs/

# display help if no args specified
CMD bin/run_reports.py --help
