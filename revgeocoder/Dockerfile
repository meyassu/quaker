# FROM python:3.9-slim
FROM continuumio/miniconda3

WORKDIR /usr/src/app

COPY . .

RUN conda env create -f environment.yml

ENV PYTHONUNBUFFERED 1

CMD ["conda", "run", "--no-capture-output", "-n", "revgeocoder", "python", "-m", "src.main"]