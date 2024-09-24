FROM python:3.8-slim

WORKDIR /app

COPY . .

EXPOSE 8080

CMD ["python", "primary.py"]
