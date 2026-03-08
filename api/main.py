"""FastAPI application: API endpoints + SPA static file serving."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.cache import get_stale_ciks, refresh_cache
from config.settings import FRONTEND_DIST_DIR
from db.database import get_connection, init_db

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB and refresh stale cache on startup."""
    init_db()
    conn = get_connection()
    try:
        stale = get_stale_ciks(conn)
        if stale:
            logger.info("Refreshing metrics cache for %d stale filers...", len(stale))
            refresh_cache(
                conn,
                progress_callback=lambda cur, tot: (
                    logger.info("Cache refresh: %d/%d", cur, tot)
                    if cur % 100 == 0 or cur == tot
                    else None
                ),
            )
            logger.info("Cache refresh complete.")
    finally:
        conn.close()
    yield


app = FastAPI(title="13F Fund Analyst", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:8000", "http://127.0.0.1:5173", "http://127.0.0.1:8000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Register API routers ---
from api.routers.stats import router as stats_router  # noqa: E402
from api.routers.funds import router as funds_router  # noqa: E402

app.include_router(stats_router, prefix="/api")
app.include_router(funds_router, prefix="/api")

# --- Static files + SPA catch-all ---
if FRONTEND_DIST_DIR.exists():
    # Serve Vite build assets
    assets_dir = FRONTEND_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the SPA index.html for all non-API routes."""
        # Try to serve a static file first
        file_path = FRONTEND_DIST_DIR / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(FRONTEND_DIST_DIR / "index.html"))
