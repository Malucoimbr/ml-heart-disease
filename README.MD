# 🚀 Guia Rápido de Execução MLOps

Este guia foca nos comandos necessários para subir e testar seu pipeline.

## 1. ⚙️ Iniciar a Infraestrutura

Na pasta raiz do projeto, execute este comando para construir as imagens (FastAPI, Jupyter) e iniciar todos os serviços:

```bash
docker-compose up -d --build
```

## 2. 🚨 Verificação e Acesso

| Serviço    | Porta | Endereço                        | Ação                                                                 |
|------------|-------|---------------------------------|----------------------------------------------------------------------|
| FastAPI    | 8000  | [http://localhost:8000/docs](http://localhost:8000/docs) | Execute o endpoint `/ingest_kaggle/` para enviar o dataset para o S3 |
| JupyterLab | 8888  | [http://localhost:8888](http://localhost:8888)           | Código ML: Use `docker-compose logs jupyterlab` para obter o token de acesso |
| MLFlow     | 5000  | [http://localhost:5000](http://localhost:5000)           | Painel de rastreamento de experimentos                               |

## 3. 🧹 Parar Tudo

Para desligar todos os contêineres e liberar a porta:

```bash
docker-compose down
```
