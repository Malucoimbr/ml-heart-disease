from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import boto3
import os
import time
import kagglehub
import glob
import shutil # Para limpar o download local após o upload

app = FastAPI(title="Camada de Ingestão de Dados (FastAPI)")

# Configuração do Boto3 (para AWS S3)
def get_s3_client():
    # As credenciais são lidas automaticamente do ambiente, injetadas pelo docker-compose
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION")
    )

@app.post("/upload_data/")
async def upload_data(file: UploadFile = File(...)):
    """[OPCIONAL] Recebe um arquivo via POST e armazena-o no S3 (dados brutos)."""
    
    s3_client = get_s3_client()
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    
    timestamp = int(time.time())
    file_extension = file.filename.split('.')[-1]
    file_name_base = file.filename.split('.')[0]
    s3_key = f"raw_data/{file_name_base}_{timestamp}.{file_extension}"
    
    try:
        file_content = await file.read()
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        
        return JSONResponse(content={
            "status": "sucesso",
            "message": "Dados brutos carregados para o S3 via upload manual.",
            "s3_path": s3_url
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "status": "erro", 
            "message": f"Falha no upload para o S3: {str(e)}"
        })

# NOVO ENDPOINT DE INGESTÃO AUTOMÁTICA
@app.post("/ingest_kaggle/")
async def ingest_kaggle_data():
    """Baixa o dataset específico de doença cardíaca do Kaggle e o armazena no S3."""
    
    # SLUG do dataset solicitado
    dataset_slug = "sid321axn/heart-statlog-cleveland-hungary-final"
    
    s3_client = get_s3_client()
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    
    # Variáveis para limpeza
    path_to_dir = None
    
    try:
        # 1. Baixar o dataset do Kaggle (cria uma pasta temporária)
        print(f"Baixando dataset: {dataset_slug}...")
        path_to_dir = kagglehub.dataset_download(dataset_slug)
        
        # 2. Encontrar o arquivo CSV principal (o dataset é uma pasta)
        csv_files = glob.glob(f"{path_to_dir}/*.csv")
        if not csv_files:
            raise Exception("Nenhum arquivo CSV principal encontrado no download do Kaggle.")
            
        local_file_path = csv_files[0]
        file_name = os.path.basename(local_file_path)
        
        # 3. Preparar caminho no S3
        timestamp = int(time.time())
        s3_key = f"raw_data/{file_name.split('.')[0]}_{timestamp}.csv"
        
        # 4. Ler o arquivo local e fazer upload para o S3
        with open(local_file_path, 'rb') as f:
            file_content = f.read()
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        
        return JSONResponse(content={
            "status": "sucesso",
            "message": f"Dataset '{dataset_slug}' baixado do Kaggle e armazenado no S3.",
            "s3_path": s3_url
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "status": "erro", 
            "message": f"Falha na ingestão do Kaggle: {str(e)}"
        })
    finally:
        # 5. Limpar arquivos temporários baixados pelo kagglehub
        if path_to_dir and os.path.exists(path_to_dir):
            shutil.rmtree(path_to_dir, ignore_errors=True)