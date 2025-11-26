# ./fastapi/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instala dependências da API (FastAPI, uvicorn, boto3 para S3)
# Inclui kagglehub para baixar o dataset
RUN pip install fastapi uvicorn "python-multipart" boto3 kagglehub

# Copia o código da API
COPY ./main.py /app/main.py

# Porta padrão do FastAPI
EXPOSE 8000

# Comando para iniciar a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]