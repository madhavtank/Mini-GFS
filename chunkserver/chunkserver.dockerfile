FROM python:3.8-slim

WORKDIR /app

COPY . .

EXPOSE ${PORT}

CMD ["python", "chunk_server.py"]
