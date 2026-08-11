"""
NOXIS Holehe Service
====================

Microservicio de Email Intelligence para NOXIS.

Este servicio encapsula Holehe y expone una API HTTP
independiente del backend principal de NOXIS.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr


app = FastAPI(
    title="NOXIS Holehe",
    description="Email Intelligence Engine for NOXIS",
    version="0.1.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class EmailSearchRequest(BaseModel):
    email: EmailStr


@app.get("/")
async def root():
    return {
        "service": "noxis-holehe",
        "engine": "holehe",
        "status": "online",
        "version": "0.1.0",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "noxis-holehe",
        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "live",
        },
    }


@app.post("/api/v1/search/email")
async def search_email(request: EmailSearchRequest):
    """
    Endpoint inicial de Email Intelligence.

    En el siguiente paso se conectará aquí el motor real
    de Holehe. Por ahora permite comprobar que el
    microservicio y su contrato HTTP funcionan.
    """

    return {
        "status": "ready",
        "email": request.email,
        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "pending_integration",
        },
        "summary": {
            "sites_checked": 0,
            "registered": 0,
            "not_registered": 0,
            "errors": 0,
        },
        "results": [],
    }
