"""
NOXIS Holehe Service
====================

Microservicio de Email Intelligence para NOXIS.

Integra Holehe y expone una API HTTP independiente
del backend principal de NOXIS.

Principios:
- Un fallo individual no detiene toda la búsqueda.
- Una coincidencia técnica no confirma identidad personal.
- Los resultados se normalizan para integración posterior con NOXIS.
"""

from __future__ import annotations

import asyncio
import importlib
import pkgutil
import time
from typing import Any, Callable, Dict, List

import httpx
import holehe.modules

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr


# ============================================================
# APP
# ============================================================

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


# ============================================================
# REQUEST MODEL
# ============================================================

class EmailSearchRequest(BaseModel):
    email: EmailStr


# ============================================================
# JSON SAFE
# ============================================================

def make_json_safe(value: Any) -> Any:
    """
    Convierte valores potencialmente no serializables
    a estructuras compatibles con JSON.
    """

    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {
            str(key): make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [
            make_json_safe(item)
            for item in value
        ]

    return str(value)


# ============================================================
# DESCUBRIMIENTO DE MÓDULOS HOLEHE
# ============================================================

def discover_holehe_modules() -> List[Callable[..., Any]]:
    """
    Descubre automáticamente los módulos instalados de Holehe.

    Estructura habitual:

    holehe.modules.<categoria>.<servicio>

    Cada archivo suele exponer una función con
    el mismo nombre que el módulo.
    """

    discovered: List[Callable[..., Any]] = []

    modules_path = getattr(
        holehe.modules,
        "__path__",
        None,
    )

    if not modules_path:
        return discovered

    for _, category_name, is_package in pkgutil.iter_modules(
        modules_path
    ):

        if not is_package:
            continue

        category_module_name = (
            f"holehe.modules.{category_name}"
        )

        try:
            category_module = importlib.import_module(
                category_module_name
            )
        except Exception:
            continue

        category_path = getattr(
            category_module,
            "__path__",
            None,
        )

        if not category_path:
            continue

        for _, module_name, module_is_package in pkgutil.iter_modules(
            category_path
        ):

            if module_is_package:
                continue

            full_module_name = (
                f"{category_module_name}.{module_name}"
            )

            try:
                imported_module = importlib.import_module(
                    full_module_name
                )
            except Exception:
                continue

            function = getattr(
                imported_module,
                module_name,
                None,
            )

            if callable(function):
                discovered.append(function)

    return discovered


HOLEHE_MODULES = discover_holehe_modules()


# ============================================================
# EJECUCIÓN DE UN MÓDULO
# ============================================================

async def run_holehe_module(
    module_function: Callable[..., Any],
    email: str,
    client: httpx.AsyncClient,
    timeout_seconds: float = 20.0,
) -> Dict[str, Any]:
    """
    Ejecuta un módulo individual.

    Holehe agrega el resultado al array 'out'.
    """

    out: List[Dict[str, Any]] = []

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
                out,
            ),
            timeout=timeout_seconds,
        )

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

    if not out:

        return {
            "site": module_name,
            "status": "error",
            "exists": None,
            "rate_limited": False,
            "error": "empty_result",
        }

    raw_result = out[0]

    if not isinstance(raw_result, dict):

        return {
            "site": module_name,
            "status": "error",
            "exists": None,
            "rate_limited": False,
            "error": "invalid_result",
        }

    exists = raw_result.get("exists")

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
            raw_result.get("name")
            or module_name
        ),

        "domain": raw_result.get("domain"),

        "method": raw_result.get("method"),

        "status": status,

        "exists": exists,

        "rate_limited": rate_limited,

        "email_recovery": raw_result.get(
            "emailrecovery"
        ),

        "phone_recovery": raw_result.get(
            "phoneNumber"
        ),

        "others": make_json_safe(
            raw_result.get("others")
        ),
    }


# ============================================================
# MOTOR PRINCIPAL
# ============================================================

async def search_holehe(
    email: str,
) -> Dict[str, Any]:
    """
    Ejecuta todos los módulos disponibles de Holehe.
    """

    started_at = time.perf_counter()

    normalized_email = (
        str(email)
        .strip()
        .lower()
    )

    if not HOLEHE_MODULES:

        return {
            "status": "error",

            "email": normalized_email,

            "engine": {
                "id": "holehe",
                "name": "Holehe",
                "mode": "live",
            },

            "error": "no_modules_loaded",

            "message": (
                "Holehe está instalado pero no fue posible "
                "descubrir módulos ejecutables."
            ),

            "summary": {
                "modules_loaded": 0,
                "sites_checked": 0,
                "registered": 0,
                "not_registered": 0,
                "rate_limited": 0,
                "unknown": 0,
                "errors": 0,
            },

            "registered_accounts": [],

            "results": [],
        }

    # --------------------------------------------------------
    # HTTP CLIENT
    # --------------------------------------------------------

    limits = httpx.Limits(
        max_connections=20,
        max_keepalive_connections=10,
    )

    timeout = httpx.Timeout(
        connect=10.0,
        read=20.0,
        write=10.0,
        pool=10.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        },
    ) as client:

        # Limitamos concurrencia para no disparar
        # todos los servicios simultáneamente.

        semaphore = asyncio.Semaphore(12)

        async def guarded_run(
            module_function: Callable[..., Any],
        ) -> Dict[str, Any]:

            async with semaphore:

                return await run_holehe_module(
                    module_function=module_function,
                    email=normalized_email,
                    client=client,
                )

        tasks = [
            guarded_run(module_function)
            for module_function in HOLEHE_MODULES
        ]

        results = await asyncio.gather(
            *tasks
        )

    # ========================================================
    # CLASIFICACIÓN
    # ========================================================

    registered_accounts = [
        item
        for item in results
        if item.get("status") == "registered"
    ]

    not_registered = [
        item
        for item in results
        if item.get("status") == "not_registered"
    ]

    rate_limited = [
        item
        for item in results
        if item.get("status") == "rate_limited"
    ]

    unknown = [
        item
        for item in results
        if item.get("status") == "unknown"
    ]

    errors = [
        item
        for item in results
        if item.get("status") == "error"
    ]

    duration = round(
        time.perf_counter() - started_at,
        2,
    )

    # ========================================================
    # STATUS GENERAL
    # ========================================================

    successful_checks = (
        len(registered_accounts)
        + len(not_registered)
        + len(unknown)
    )

    if successful_checks == 0:
        overall_status = "error"

    elif errors or rate_limited:
        overall_status = "partial"

    else:
        overall_status = "completed"

    # ========================================================
    # RESPONSE
    # ========================================================

    return {
        "status": overall_status,

        "email": normalized_email,

        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "live",
        },

        "duration_seconds": duration,

        "summary": {
            "modules_loaded": len(
                HOLEHE_MODULES
            ),

            "sites_checked": len(
                results
            ),

            "registered": len(
                registered_accounts
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

        # Prioridad para NOXIS.
        "registered_accounts": registered_accounts,

        # Resultado completo para auditoría.
        "results": results,

        "evidence": {
            "account_presence": {
                "status": (
                    "available"
                    if registered_accounts
                    else "none"
                ),

                "count": len(
                    registered_accounts
                ),

                "technical_match": (
                    len(registered_accounts) > 0
                ),

                # Muy importante:
                # una cuenta técnicamente asociada a un email
                # no confirma quién es la persona.
                "identity_confirmed": False,

                "description": (
                    "Holehe detecta presencia técnica del email "
                    "en servicios externos. Esto no confirma "
                    "la identidad de la persona propietaria."
                ),
            }
        },
    }


# ============================================================
# ENDPOINT ROOT
# ============================================================

@app.get("/")
async def root() -> Dict[str, Any]:

    return {
        "service": "noxis-holehe",

        "status": "online",

        "version": "0.2.0",

        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "live",
        },

        "modules_loaded": len(
            HOLEHE_MODULES
        ),
    }


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health() -> Dict[str, Any]:

    modules_loaded = len(
        HOLEHE_MODULES
    )

    return {
        "status": (
            "ok"
            if modules_loaded > 0
            else "degraded"
        ),

        "service": "noxis-holehe",

        "engine": {
            "id": "holehe",
            "name": "Holehe",
            "mode": "live",
        },

        "version": "0.2.0",

        "modules_loaded": modules_loaded,
    }


# ============================================================
# EMAIL SEARCH
# ============================================================

@app.post("/api/v1/search/email")
async def search_email(
    request: EmailSearchRequest,
) -> Dict[str, Any]:

    return await search_holehe(
        email=str(request.email)
    )
