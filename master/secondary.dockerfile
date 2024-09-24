FROM python:3.8-slim

WORKDIR /app

COPY . .

EXPOSE 8081

CMD ["python", "secondary.py"]
