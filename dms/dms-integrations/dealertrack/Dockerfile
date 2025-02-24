FROM public.ecr.aws/lambda/python:3.9
WORKDIR /app
COPY app/ .
RUN pip install -r requirements.txt
RUN chmod +x historical_pull.py
ENTRYPOINT ["python", "historical_pull.py"]
