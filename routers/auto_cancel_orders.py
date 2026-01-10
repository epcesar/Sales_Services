from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
import logging
import asyncio
import httpx
import os
from database import get_db_connection

logger = logging.getLogger(__name__)

router_auto_cancel = APIRouter(
    prefix="/auto-cancel",
    tags=["Auto Cancel Orders"]
)

# Blockchain Configuration
BLOCKCHAIN_LOG_URL = os.getenv("BLOCKCHAIN_LOG_URL", "http://localhost:9005/blockchain/log")

# Flag to control the background task
_background_task_running = False
_background_task = None

async def log_to_blockchain_system(
    action: str,
    entity_id: int,
    change_description: str,
    data: dict
):
    """
    Log system actions to blockchain without authentication token.
    Uses system credentials or unauthenticated endpoint.
    """
    # Check if blockchain logging is enabled
    if not BLOCKCHAIN_LOG_URL or BLOCKCHAIN_LOG_URL == "disabled":
        logger.info(f"ℹ️  Blockchain logging disabled - skipping log for {action} on Sale {entity_id}")
        return None
    
    payload = {
        "service_identifier": "POS_SALES_AUTO_CANCEL",
        "action": action,
        "entity_type": "Sale",
        "entity_id": entity_id,
        "actor_username": "System",  # System user for automated actions
        "change_description": change_description,
        "data": data
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Try without authentication first (for system actions)
            # If your blockchain service requires auth, you'll need to add a system token
            response = await client.post(BLOCKCHAIN_LOG_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            logger.info(f"✅ Blockchain log recorded: {action} on Sale {entity_id} (TX: {result.get('transaction_hash', 'N/A')})")
            return result
    except httpx.ConnectError as e:
        logger.warning(f"⚠️  Blockchain service unavailable at {BLOCKCHAIN_LOG_URL}: {e}")
    except httpx.TimeoutException as e:
        logger.warning(f"⚠️  Blockchain logging timeout: {e}")
    except httpx.RequestError as e:
        logger.warning(f"⚠️  Blockchain logging failed (network error): {e}")
    except httpx.HTTPStatusError as e:
        logger.warning(f"⚠️  Blockchain logging failed (HTTP {e.response.status_code}): {e.response.text}")
    except Exception as e:
        logger.warning(f"⚠️  Blockchain logging failed: {e}")
    
    return None

async def auto_cancel_expired_orders():
    """
    Background task that runs every 5 minutes to check for pending orders
    older than 30 minutes and automatically cancels them.
    """
    while _background_task_running:
        try:
            logger.info("🔍 Checking for expired pending orders...")
            conn = await get_db_connection()
            
            async with conn.cursor() as cursor:
                # Find all pending orders older than 30 minutes
                # Calculate total from SaleItems and get cashier name from CashierSessions
                expiration_time = datetime.now() - timedelta(minutes=30)
                
                await cursor.execute("""
                    SELECT 
                        s.SaleID, 
                        s.GCashReferenceNumber, 
                        s.OrderType,
                        s.CreatedAt,
                        ISNULL(cs.CashierName, 'Unknown') AS CashierName,
                        -- Calculate total from SaleItems
                        ISNULL((
                            SELECT SUM(si.UnitPrice * si.Quantity)
                            FROM SaleItems si
                            WHERE si.SaleID = s.SaleID
                        ), 0) AS CalculatedTotal
                    FROM Sales s
                    LEFT JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                    WHERE s.Status = 'pending' 
                    AND s.CreatedAt < ?
                """, expiration_time)
                
                expired_orders = await cursor.fetchall()
                
                if expired_orders:
                    logger.info(f"Found {len(expired_orders)} expired pending orders")
                    
                    for order in expired_orders:
                        sale_id = order.SaleID
                        reference_number = order.GCashReferenceNumber
                        order_type = order.OrderType
                        created_at = order.CreatedAt
                        cashier_name = order.CashierName
                        total_amount = order.CalculatedTotal  # Use calculated total
                        
                        try:
                            # Update status to cancelled
                            await cursor.execute("""
                                UPDATE Sales 
                                SET Status = 'cancelled', UpdatedAt = GETDATE() 
                                WHERE SaleID = ?
                            """, sale_id)
                            
                            # Log the cancellation
                            await cursor.execute("""
                                INSERT INTO CancelledOrders 
                                (SaleID, ManagerUsername, CancelledAt)
                                VALUES (?, 'SYSTEM_AUTO_CANCEL', GETDATE())
                            """, sale_id)
                            
                            await conn.commit()
                            
                            logger.info(
                                f"✅ Auto-cancelled POS order SaleID={sale_id}, "
                                f"Ref={reference_number}, Type={order_type}, Cashier={cashier_name}"
                            )
                            
                            # Log to blockchain
                            time_elapsed = datetime.now() - created_at
                            minutes_elapsed = int(time_elapsed.total_seconds() / 60)
                            
                            blockchain_data = {
                                "sale_id": sale_id,
                                "reference_number": reference_number,
                                "order_type": order_type,
                                "total_amount": str(total_amount) if total_amount else "0.00",
                                "original_cashier": cashier_name,
                                "created_at": created_at.isoformat(),
                                "cancelled_at": datetime.now().isoformat(),
                                "minutes_elapsed": minutes_elapsed,
                                "cancellation_reason": "Order expired after 30 minutes"
                            }
                            
                            await log_to_blockchain_system(
                                action="AUTO_CANCEL",
                                entity_id=sale_id,
                                change_description=f"Automatically cancelled order after {minutes_elapsed} minutes (threshold: 30 min) - Ref: {reference_number or 'N/A'} - Cashier: {cashier_name}",
                                data=blockchain_data
                            )
                            
                            # If it's an online order (has reference number), notify OOS
                            if reference_number and order_type.lower() in ['delivery', 'pick-up']:
                                try:
                                    async with httpx.AsyncClient(timeout=10.0) as client:
                                        # Call OOS to cancel the corresponding order
                                        response = await client.patch(
                                            f"http://localhost:7004/cart/admin/orders/auto-cancel/{reference_number}",
                                            json={
                                                "reason": "Order expired after 30 minutes",
                                                "cancelled_by": "SYSTEM_AUTO_CANCEL"
                                            }
                                        )
                                        
                                        if response.status_code == 200:
                                            logger.info(f"✅ Notified OOS to cancel order {reference_number}")
                                        else:
                                            logger.warning(
                                                f"⚠️  Failed to notify OOS: {response.status_code} - {response.text}"
                                            )
                                            
                                except httpx.RequestError as e:
                                    logger.error(f"❌ Failed to notify OOS for {reference_number}: {e}")
                            
                        except Exception as e:
                            await conn.rollback()
                            logger.error(f"❌ Failed to auto-cancel SaleID={sale_id}: {e}", exc_info=True)
                            continue
                else:
                    logger.info("✓ No expired pending orders found")
                    
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Error in auto-cancel task: {e}", exc_info=True)
        
        # Wait 5 minutes before next check
        await asyncio.sleep(300)  


@router_auto_cancel.post("/start")
async def start_auto_cancel_task():
    """Start the automatic order cancellation background task"""
    global _background_task_running, _background_task
    
    if _background_task_running:
        return {"message": "Auto-cancel task is already running"}
    
    _background_task_running = True
    _background_task = asyncio.create_task(auto_cancel_expired_orders())
    
    logger.info("✅ Started auto-cancel background task")
    return {"message": "Auto-cancel task started successfully"}


@router_auto_cancel.post("/stop")
async def stop_auto_cancel_task():
    """Stop the automatic order cancellation background task"""
    global _background_task_running, _background_task
    
    if not _background_task_running:
        return {"message": "Auto-cancel task is not running"}
    
    _background_task_running = False
    
    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass
    
    logger.info("✅ Stopped auto-cancel background task")
    return {"message": "Auto-cancel task stopped successfully"}


@router_auto_cancel.get("/status")
async def get_auto_cancel_status():
    """Check if the auto-cancel task is running"""
    return {
        "running": _background_task_running,
        "message": "Auto-cancel task is " + ("running" if _background_task_running else "stopped")
    }