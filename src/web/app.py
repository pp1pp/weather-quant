"""FastAPI application factory for the weather-quant dashboard API."""

import base64
import secrets

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
import os

from src.web.routes import forecast, market, probabilities, positions, stats, bias, timing, dashboard, mode, backtest, calibrate


class BasicAuthMiddleware(BaseHTTPMiddleware):
    """Simple HTTP Basic Auth middleware. Skips /api/health for container healthchecks."""

    def __init__(self, app, password: str):
        super().__init__(app)
        self.password = password

    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/api/health":
            return await call_next(request)

        auth = request.headers.get("authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                _, pwd = decoded.split(":", 1)
                if secrets.compare_digest(pwd, self.password):
                    return await call_next(request)
            except Exception:
                pass

        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Weather Quant Dashboard"'},
            content="Unauthorized",
        )


def create_app(modules: dict, config: dict) -> FastAPI:
    """Create FastAPI app wired to the trading system modules."""
    app = FastAPI(
        title="Weather Quant Dashboard API",
        version="1.0.0",
    )

    # Basic Auth — only when DASHBOARD_PASSWORD is set
    dash_password = os.getenv("DASHBOARD_PASSWORD", "")
    if dash_password:
        app.add_middleware(BasicAuthMiddleware, password=dash_password)

    # CORS for Vite dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )

    # Store modules & config on app state for route access
    app.state.modules = modules
    app.state.config = config

    # Health check — register BEFORE routers, exempt from auth
    @app.get("/api/health")
    def health():
        try:
            db = modules.get("db")
            if db:
                db.execute("SELECT 1")
            return {"status": "ok"}
        except Exception:
            return JSONResponse({"status": "unhealthy"}, status_code=503)

    # Register API routes
    app.include_router(dashboard.router, prefix="/api")
    app.include_router(forecast.router, prefix="/api")
    app.include_router(market.router, prefix="/api")
    app.include_router(probabilities.router, prefix="/api")
    app.include_router(positions.router, prefix="/api")
    app.include_router(stats.router, prefix="/api")
    app.include_router(bias.router, prefix="/api")
    app.include_router(timing.router, prefix="/api")
    app.include_router(mode.router, prefix="/api")
    app.include_router(backtest.router, prefix="/api")
    app.include_router(calibrate.router, prefix="/api")

    # Serve built frontend — mount static assets at /assets, NOT at /
    # This prevents the catch-all from swallowing /api routes
    dist_path = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "dist")
    if os.path.isdir(dist_path):
        assets_path = os.path.join(dist_path, "assets")
        if os.path.isdir(assets_path):
            app.mount("/assets", StaticFiles(directory=assets_path), name="static_assets")

        # Serve favicon
        favicon_path = os.path.join(dist_path, "favicon.svg")

        @app.get("/favicon.svg")
        def favicon():
            if os.path.exists(favicon_path):
                return FileResponse(favicon_path, media_type="image/svg+xml")

        # SPA fallback: serve index.html for all non-API, non-asset routes
        index_path = os.path.join(dist_path, "index.html")

        @app.get("/{full_path:path}")
        def spa_fallback(full_path: str):
            if os.path.exists(index_path):
                return FileResponse(index_path, media_type="text/html")
            return {"error": "frontend not built"}

    return app
