from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json
import sys
import os

# Adiciona o diretório raiz ao path para encontrar o módulo 'core'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.data_processor import data_processor

app = FastAPI()

# Monta o diretório 'static' para servir os arquivos do frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def read_root():
    # Serve o arquivo HTML principal
    return FileResponse('static/index.html')

@app.get("/api/geral")
async def get_geral_data():
    return data_processor.get_geral_data()

@app.get("/api/municipios")
async def get_municipios():
    return data_processor.get_municipio_list()

@app.get("/api/municipio/{nome_municipio}")
async def get_municipio_data(nome_municipio: str):
    data = data_processor.get_data_by_municipio(nome_municipio)
    if data is None:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    return data

@app.get("/api/mapa")
async def get_map_data():
    # Retorna o GeoJSON processado como uma resposta JSON
    map_json = data_processor.get_map_data()
    return Response(content=map_json, media_type="application/json")

@app.get("/api/ranking/{metric}")
async def get_ranking(metric: str):
    # As métricas podem ser: 'vulnerabilidade', 'renda', 'alfabetizacao', 'populacao'
    data = data_processor.get_ranking_by_metric(metric)
    if data is None:
        raise HTTPException(status_code=400, detail="Métrica inválida")
    return data