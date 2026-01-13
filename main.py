from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio

# --- Imports for your routers ---
from routers import pos_router, purchase_order, cancelled_order, sales
from routers.top_products import router_top_products
from routers.transaction_history import router_transaction_history
from routers.auto_cancel_orders import router_auto_cancel, auto_cancel_expired_orders
from routers.refund import router_refund  # Import the router instance
from routers.dashboard import router_dashboard  # Import the router instance

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
    lifespan=lifespan  # Register lifespan handler
)

# --- Include all your routers ---
app.include_router(pos_router.router_sales)
app.include_router(purchase_order.router_purchase_order)
app.include_router(cancelled_order.router_cancelled_order)
app.include_router(router_top_products)
app.include_router(router_transaction_history)
app.include_router(sales.router_sales_metrics)
app.include_router(router_auto_cancel)  
app.include_router(router_refund)  
app.include_router(router_dashboard)  

# --- CORS middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        '*',  # Self
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# "https://bleu-pos-tau.vercel.app",
#         "https://bleu-ims-beta.vercel.app",
#         "https://authservices-npr8.onrender.com",
#         "https://bleu-stockservices.onrender.com",
#         "https://ims-restockservices.onrender.com",
#         "https://bleu-oos-rouge.vercel.app",
#         "https://ordering-service-8e9d.onrender.com",  # Add OOS service
#         "https://blockchainservices.onrender.com"
# --- Health check endpoint ---
@app.get("/", tags=["Health Check"])
def read_root():
    return {
        "status": "ok", 
        "message": "POS Service is running.",
        "auto_cancel_enabled": _task_running
    }

# --- Uvicorn runner ---
if __name__ == "__main__":
    import uvicorn
    
    print("--- Starting POS Service on http://0.0.0.0:9000 ---")
    print("API docs available at http://127.0.0.1:9000/docs")
    print("Auto-cancel will start automatically on startup")
    uvicorn.run("main:app", port=9000, host="0.0.0.0", reload=True)