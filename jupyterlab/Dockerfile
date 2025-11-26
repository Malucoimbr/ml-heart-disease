# Usa uma imagem base estável com Python e JupyterLab
FROM jupyter/scipy-notebook

# Instalação das bibliotecas necessárias
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    mlflow \
    boto3 \
    psycopg2-binary \
    s3fs \
    fastapi uvicorn 
    
WORKDIR /home/jovyan/work