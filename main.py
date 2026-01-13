from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio

# --- Imports for your routers ---
from routers import pos_router, purchase_order, cancelled_order, sales
from routers.top_products import router_top_products
from routers.transaction_history import router_transaction_history
from routers.auto_cancel_orders import router_auto_cancel, auto_cancel_expired_orders
from routers.refund import router_refund
from routers.dashboard import router_dashboard

# Global flag for background task
_auto_cancel_task = None
_task_running = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager to start/stop background tasks
    """
    global _auto_cancel_task, _task_running
    
    # Startup: Start the auto-cancel background task
    print("🚀 Starting auto-cancel background task...")
    _task_running = True
    
    # Import the global flags from the router
    import routers.auto_cancel_orders as aco
    aco._background_task_running = True
    _auto_cancel_task = asyncio.create_task(auto_cancel_expired_orders())
    
    print("✅ Auto-cancel task started")
    
    yield  # Application runs here
    
    # Shutdown: Stop the background task
    print("🛑 Stopping auto-cancel background task...")
    _task_running = False
    aco._background_task_running = False
    
    if _auto_cancel_task:
        _auto_cancel_task.cancel()
        try:
            await _auto_cancel_task
        except asyncio.CancelledError:
            pass
    
    print("✅ Auto-cancel task stopped")

app = FastAPI(
    title="POS and Order Service API",
    description="Handles sales creation and retrieves processing orders.",
    version="1.0.0",
    lifespan=lifespan
)

# --- CRITICAL: Add CORS middleware BEFORE including routers ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bleu-pos-tau.vercel.app",
        "https://bleu-ims-beta.vercel.app",
        "https://authservices-npr8.onrender.com",
        "https://bleu-stockservices.onrender.com",
        "https://ims-restockservices.onrender.com",
        "https://bleu-oos-rouge.vercel.app",
        "https://ordering-service-8e9d.onrender.com",
        "https://blockchainservices.onrender.com",
        "https://notificationservice-1jp5.onrender.com",
        "http://localhost:3000",  # For local development
        "http://localhost:5173",  # For Vite dev server
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],  # Important for some headers to be visible to frontend
)

# --- Register routers with DUAL paths for backward compatibility ---
# This allows both old (/auth/sales) and new (/auth/purchase_orders) paths to work

# Primary router registration (new path)
app.include_router(
    purchase_order.router_purchase_order,
    prefix="/auth/purchase_orders",
    tags=["Purchase Orders"]
)

# Backward compatibility registration (old path)
# This makes /auth/sales/status/{status} work
app.include_router(
    purchase_order.router_purchase_order,
    prefix="/auth/sales",
    tags=["Sales (Legacy)"]
)

# Include other routers
app.include_router(pos_router.router_sales)
app.include_router(cancelled_order.router_cancelled_order)
app.include_router(router_top_products)
app.include_router(router_transaction_history)
app.include_router(sales.router_sales_metrics)
app.include_router(router_auto_cancel)
app.include_router(router_refund)
app.include_router(router_dashboard)

# --- Health check endpoint ---
@app.get("/", tags=["Health Check"])
def read_root():
    return {
        "status": "ok", 
        "message": "POS Service is running.",
        "auto_cancel_enabled": _task_running,
        "service": "Sales Service API",
        "version": "1.0.0"
    }

# --- Additional health check for monitoring ---
@app.get("/health", tags=["Health Check"])
def health_check():
    """
    Detailed health check endpoint for monitoring services
    """
    return {
        "status": "healthy",
        "service": "sales-services",
        "auto_cancel_task_running": _task_running,
        "endpoints_available": True
    }

# --- Uvicorn runner ---
if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("🚀 Starting POS Service")
    print("=" * 60)
    print("📍 Server: http://0.0.0.0:9000")
    print("📚 API Docs: http://127.0.0.1:9000/docs")
    print("🔄 Auto-cancel: Enabled")
    print("🌐 CORS: Configured for production and development")
    print("=" * 60)
    
    uvicorn.run(
        "main:app", 
        port=9000, 
        host="0.0.0.0", 
        reload=True,
        log_level="info"
    )