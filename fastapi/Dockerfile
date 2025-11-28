# ./fastapi/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# INSTALAR DEPENDÊNCIAS DE DADOS E CONEXÃO SQL:
# Adicionamos: pandas, sqlalchemy (para create_engine) e psycopg2-binary (driver do Postgres)
RUN pip install fastapi uvicorn "python-multipart" boto3 kagglehub pandas sqlalchemy psycopg2-binary

# Copia o código da API
COPY ./main.py /app/main.py

# Porta padrão do FastAPI
EXPOSE 8000

# Comando para iniciar a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]