from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sys
import os


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Adiciona a raiz do projeto ao path do Python para encontrar o módulo 'core'
sys.path.append(PROJECT_ROOT)

from core.data_processor import data_processor

app = FastAPI()

# Constrói o caminho absoluto para a pasta 'static'
static_path = os.path.join(PROJECT_ROOT, 'static')

# Monta o diretório 'static' usando o caminho absoluto
app.mount("/static", StaticFiles(directory=static_path), name="static")

@app.get("/")
async def read_root():
    # Serve o arquivo HTML principal a partir do caminho absoluto
    return FileResponse(os.path.join(static_path, 'index.html'))

# --- As rotas abaixo não precisam de alteração ---

@app.get("/api/geral")
async def get_geral_data(age_group: str = 'geral'):
    return data_processor.get_geral_data(age_group)

@app.get("/api/municipios")
async def get_municipios():
    return data_processor.get_municipio_list()

@app.get("/api/municipio/{nome_municipio}")
async def get_municipio_data(nome_municipio: str, age_group: str = 'geral'):
    data = data_processor.get_data_by_municipio(nome_municipio, age_group)
    if data is None:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    return data

@app.get("/api/mapa")
async def get_map_data(age_group: str = 'geral'):
    return data_processor.get_map_data(age_group)

@app.get("/api/ranking/{metric}")
async def get_ranking(metric: str, age_group: str = 'geral'):
    data = data_processor.get_ranking_by_metric(metric, age_group)
    if data is None:
        raise HTTPException(status_code=400, detail="Métrica inválida")
    return data