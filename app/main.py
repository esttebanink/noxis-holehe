"""
NOXIS Holehe Service
====================

Microservicio de Email Intelligence para NOXIS.

Integra Holehe de forma real y devuelve resultados normalizados
para consumo posterior desde NOXIS API.
"""

from __future__ import annotations

import asyncio
import importlib
import pkgutil
import time
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

import holehe.modules


app = FastAPI(
    title="NOXIS Holehe",
    description="Email Intelligence Engine for NOXIS",
    version="0.2.0",
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


# ================================================================
# UTILIDADES
# ================================================================


def make_json_safe(value: Any) -> Any:
    """
    Convierte estructuras devueltas por Holehe
    a valores seguros para JSON.
    """

    if value is None:
        return None

    if isinstance(
        value,
        (str, int, float, bool),
    ):
        return value

    if isinstance(value, dict):
        return {
            str(key): make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(
        value,
        (list, tuple, set),
    ):
        return [
            make_json_safe(item)
            for item in value
        ]

    return str(value)


def discover_holehe_modules() -> List[Any]:
    """
    Descubre automáticamente los módulos disponibles
    dentro de holehe.modules.

    Holehe organiza sus módulos en subpaquetes.
    Evitamos mantener una lista manual.
    """

    functions: List[Any] = []

    for _, category_name, is_pkg in pkgutil.iter_modules(
        holehe.modules.__path__
    ):

        if not is_pkg:
            continue

        category_path = (
            f"holehe.modules.{category_name}"
        )

        try:
            category = importlib.import_module(
                category_path
            )
        except Exception:
            continue

        if not hasattr(
            category,
            "__path__",
        ):
            continue

        for _, module_name, module_is_pkg in pkgutil.iter_modules(
            category.__path__
        ):

            if module_is_pkg:
                continue

            full_module_name = (
                f"{category_path}.{module_name}"
            )

            try:
                module = importlib.import_module(
                    full_module_name
                )
            except Exception:
                continue

            function = getattr(
                module,
                module_name,
                None,
            )

            if callable(function):
                functions.append(
                    function
                )

    return functions


HOLEHE_MODULES = discover_holehe_modules()


async def run_holehe_module(
    module_function: Any,
    email: str,
    client: httpx.AsyncClient,
    timeout_seconds: float = 15.0,
) -> Dict[str, Any]:
    """
    Ejecuta un módulo individual de Holehe.

    Un fallo de un proveedor no debe detener
    el análisis completo.
    """

    output: List[Dict[str, Any]] = []

    module_name = getattr(
        module_function,
        "__name__",
        "unknown",
    )

    try:

        await asyncio.wait_for(
            module_function(
                email,
                client,
                output,
            ),
            timeout=timeout_seconds,
        )

        if not output:

            return {
                "site": module_name,
                "status": "error",
                "exists": None,
                "rate_limited": False,
                "error": "empty_result",
            }

        raw_result = output[0]

        if not isinstance(
            raw_result,
            dict,
        ):

            return {
                "site": module_name,
                "status": "error",
                "exists": None,
                "rate_limited": False,
                "error": "invalid_result",
            }

        exists = raw_result.get(
            "exists"
        )

        rate_limited = bool(
            raw_result.get(
                "rateLimit",
                False,
            )
        )

        if rate_limited:

            status = "rate_limited"

        elif exists is True:

            status = "registered"

        elif exists is False:

            status = "not_registered"

        else:

            status = "unknown"

        return {
            "site": (
                raw_result.get(
                    "name"
                )
                or module_name
            ),

            "status": status,

            "exists": exists,

            "rate_limited": rate_limited,

            "email_recovery": (
                raw_result.get(
                    "emailrecovery"
                )
            ),

            "phone_recovery": (
                raw_result.get(
                    "phoneNumber"
                )
            ),

            "others": make_json_safe(
                raw_result.get(
                    "others"
                )
            ),
        }

    except asyncio.TimeoutError:

        return {
            "site": module_name,
            "status": "error",
            "exists": None,
            "rate_limited": False,
            "error": "timeout",
        }

    except Exception as exc:

        return {
            "site": module_name,
            "status": "error",
            "exists": None,
            "rate_limited": False,
            "error": "module_exception",
            "detail": str(exc),
        }


async def search_holehe(
    email: str,
) -> Dict[str, Any]:
    """
    Ejecuta todos los módulos disponibles de Holehe.
    """

    started_at = time.perf_counter()

    if not HOLEHE_MODULES:

        return {
            "status": "error",

            "engine": {
                "id": "holehe",
                "name": "Holehe",
                "mode": "live",
            },

            "email": email,

            "error": "no_modules_loaded",

            "summary": {
                "sites_checked": 0,
                "registered": 0,
                "not_registered": 0,
                "rate_limited": 0,
                "errors": 0,
            },

            "results": [],
        }

    limits = httpx.Limits(
        max_connections=25,
        max_keepalive_connections=10,
    )

    timeout = httpx.Timeout(
        connect=10.0,
        read=15.0,
        write=10.0,
        pool=10.0,
    )

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
        limits=limits,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/120 Safari/537.36"
            )
        },
    ) as client:

        semaphore = asyncio.Semaphore(15)

        async def guarded_run(
            module_function: Any,
        ) -> Dict[str, Any]:

            async with semaphore:

                return await run_holehe_module(
                    module_function,
                    email,
                    client,
                )

        tasks = [
            guarded_run(
                module_function
            )
            for module_function in HOLEHE_MODULES
        ]

        results = await asyncio.gather(
            *tasks
        )

    registered = [
        item
        for item in results
        if item.get("status")
        == "registered"
    ]

    not_registered = [
        item
        for item in results
        if item.get("status")
        == "not_registered"
    ]

    rate_limited = [
        item
        for item in results
        if item.get("status")
        == "rate_limited"
    ]

    errors = [
        item
        for item in results
        if item.get("status")
        == "error"
    ]

    unknown = [
        item
        for item in results
        if item.get("status")
        == "unknown"
    ]

    duration = round(
        time.perf_counter()
        - started_at,
        2,
    )

    if registered:

        overall_status = "completed"

    elif (
        len(errors)
        + len(rate_limited)
        >= len(results)
    ):

        overall_status = "partial"

    else:

        overall_status = "completed"

    return {
        "status": overall_status,

        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "live",
        },

        "email": email,

        "duration_seconds": duration,

        "summary": {
            "sites_checked": len(
                results
            ),

            "registered": len(
                registered
            ),

            "not_registered": len(
                not_registered
            ),

            "rate_limited": len(
                rate_limited
            ),

            "unknown": len(
                unknown
            ),

            "errors": len(
                errors
            ),
        },

        # Resultados positivos separados
        # para que NOXIS pueda priorizarlos.
        "registered_accounts": (
            registered
        ),

        # Respuesta completa para auditoría
        # y futuras correlaciones.
        "results": results,

        "evidence": {
            "account_presence": {
                "status": (
                    "available"
                    if registered
                    else "none"
                ),

                "count": len(
                    registered
                ),

                # Holehe indica presencia técnica
                # en un servicio.
                # No confirma identidad personal.
                "identity_confirmed": False,

                "description": (
                    "Servicios donde Holehe detectó "
                    "una posible cuenta asociada al email. "
                    "La coincidencia técnica no confirma "
                    "identidad personal."
                ),
            }
        },
    }


# ================================================================
# ENDPOINTS
# ================================================================


@app.get("/")
async def root():

    return {
        "service": "noxis-holehe",

        "engine": "holehe",

        "status": "online",

        "version": "0.2.0",

        "modules_loaded": len(
            HOLEHE_MODULES
        ),
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

        "modules_loaded": len(
            HOLEHE_MODULES
        ),
    }


@app.post(
    "/api/v1/search/email"
)
async def search_email(
    request: EmailSearchRequest,
):

    return await search_holehe(
        str(request.email)
    )
