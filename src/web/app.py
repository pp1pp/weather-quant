"""FastAPI application factory for the weather-quant dashboard API."""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os

from src.web.routes import forecast, market, probabilities, positions, stats, bias, timing, dashboard, mode, backtest, calibrate


def create_app(modules: dict, config: dict) -> FastAPI:
    """Create FastAPI app wired to the trading system modules."""
    app = FastAPI(
        title="Weather Quant Dashboard API",
        version="1.0.0",
    )

    # CORS for Vite dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store modules & config on app state for route access
    app.state.modules = modules
    app.state.config = config

    # Health check — register BEFORE routers
    @app.get("/api/health")
    def health():
        return {"status": "ok"}

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
