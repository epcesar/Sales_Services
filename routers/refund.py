from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, validator
from typing import Optional, List
from decimal import Decimal
import logging
from datetime import datetime, timedelta
import sys
import os

# --- Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Ensure the database module can be found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_connection

# --- Auth Configuration ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://authservices-npr8.onrender.com/auth/token")
USER_SERVICE_ME_URL = "https://authservices-npr8.onrender.com/auth/users/me"
USER_EMPLOYEE_NAME_URL = "https://authservices-npr8.onrender.com/users/employee_name"
BLOCKCHAIN_LOG_URL = "https://blockchainservices.onrender.com/blockchain/log"

# --- Define the refund router ---
router_refund = APIRouter(
    prefix="/auth/purchase_orders",
    tags=["Refunds"]
)

# --- Authorization Helper Function ---
async def get_current_active_user(token: str = Depends(oauth2_scheme)):
    import httpx
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(USER_SERVICE_ME_URL, headers={"Authorization": f"Bearer {token}"})
            response.raise_for_status()
            user_data = response.json()
            user_data['access_token'] = token 
            return user_data
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Invalid token or user not found: {e.response.text}", headers={"WWW-Authenticate": "Bearer"})
        except httpx.RequestError:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Could not connect to the authentication service.")

# --- Helper: Get Full Name from Username ---
async def get_full_name_from_username(username: str, token: str) -> str:
    """Fetch the full name of a user given their username."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                USER_EMPLOYEE_NAME_URL,
                params={"username": username},
                headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            data = response.json()
            full_name = data.get("full_name", username)
            logger.info(f"✅ Retrieved full name for '{username}': {full_name}")
            return full_name
    except Exception as e:
        logger.error(f"❌ Failed to get full name for username '{username}': {e}")
        return username  # Fallback to username if service fails

# --- Blockchain Logging Helper (FIXED) ---
async def log_to_blockchain(
    service_identifier: str,
    action: str,
    entity_type: str,
    entity_id: int,
    actor_username: str,
    change_description: str,
    data: dict,
    token: str
):
    """
    Send activity logs to the blockchain service asynchronously.
    
    FIXED ISSUES:
    1. Added proper timeout handling
    2. Added retry logic
    3. Better error handling and logging
    4. Ensured data is JSON serializable
    5. Added validation before sending
    """
    import httpx
    import json
    
    # Validate inputs
    if not all([service_identifier, action, entity_type, actor_username, change_description]):
        logger.error("❌ Blockchain logging failed: Missing required fields")
        return
    
    try:
        # Ensure data is JSON serializable
        try:
            json.dumps(data)
        except (TypeError, ValueError) as e:
            logger.error(f"❌ Data is not JSON serializable: {e}")
            # Convert problematic types
            data = {k: str(v) if isinstance(v, (Decimal, datetime)) else v for k, v in data.items()}
        
        payload = {
            "service_identifier": service_identifier,
            "action": action,
            "entity_type": entity_type,
            "entity_id": int(entity_id),  # Ensure it's an int
            "actor_username": actor_username,
            "change_description": change_description,
            "data": data
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"🔗 Attempting blockchain log: {action} on {entity_type} {entity_id}")
        
        # Use longer timeout and retry logic
        async with httpx.AsyncClient(timeout=60.0) as client:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = await client.post(
                        BLOCKCHAIN_LOG_URL, 
                        json=payload, 
                        headers=headers
                    )
                    response.raise_for_status()
                    result = response.json()
                    
                    logger.info(
                        f"✅ Blockchain log created: TX {result.get('transaction_hash')} "
                        f"for {entity_type} ID {entity_id} (Attempt {attempt + 1}/{max_retries})"
                    )
                    return result  # Success, exit function
                    
                except httpx.TimeoutException:
                    logger.warning(f"⏱️  Blockchain logging timeout (Attempt {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Blockchain logging failed after {max_retries} attempts (timeout)")
                        
                except httpx.HTTPStatusError as e:
                    logger.error(
                        f"❌ Blockchain service returned error (Attempt {attempt + 1}/{max_retries}): "
                        f"{e.response.status_code} - {e.response.text}"
                    )
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Blockchain logging failed after {max_retries} attempts (HTTP error)")
                    
                except Exception as e:
                    logger.error(f"❌ Unexpected error in blockchain logging (Attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Blockchain logging failed after {max_retries} attempts (unexpected error)")
                
                # Wait before retry (exponential backoff)
                if attempt < max_retries - 1:
                    import asyncio
                    await asyncio.sleep(2 ** attempt)  # 1s, 2s, 4s
                    
    except Exception as e:
        logger.error(f"❌ Critical error in blockchain logging for {entity_type} {entity_id}: {e}", exc_info=True)

# --- Pydantic Models ---
class RefundOrderRequest(BaseModel):
    managerUsername: str
    refundReason: Optional[str] = "Customer requested refund"

class RefundItemRequest(BaseModel):
    saleItemId: int
    refundQuantity: int
    itemName: str
    originalQuantity: int
    unitPrice: float
    
    @validator('refundQuantity')
    def validate_refund_quantity(cls, v, values):
        if v <= 0:
            raise ValueError('Refund quantity must be greater than 0')
        if 'originalQuantity' in values and v > values['originalQuantity']:
            raise ValueError('Refund quantity cannot exceed original quantity')
        return v

class PartialRefundOrderRequest(BaseModel):
    managerUsername: str
    refundReason: Optional[str] = "Customer requested partial refund"
    items: List[RefundItemRequest]
    
    @validator('items')
    def validate_items(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one item must be selected for refund')
        return v

# --- REFUND ENDPOINTS ---

@router_refund.post(
    "/{order_id}/refund",
    status_code=status.HTTP_200_OK,
    summary="Process a full refund for a completed order (within 30 minutes)"
)
async def refund_order(
    order_id: str,
    request: RefundOrderRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Process a full refund for a completed order.
    Only allows refunding orders that have status 'completed'.
    Refunds must be processed within 30 minutes of order completion.
    Requires manager authorization.
    Creates entries in both RefundedOrders and RefundedItems tables.
    """
    allowed_roles = ["cashier", "admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to process refunds."
        )
    
    try:
        parsed_id = int(order_id.split('-')[-1])
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid order ID format.")
    
    conn = None
    try:
        conn = await get_db_connection()
        conn.autocommit = False
        
        async with conn.cursor() as cursor:
            # Check order exists and is completed
            await cursor.execute(
                "SELECT Status, TotalDiscountAmount, UpdatedAt, CreatedAt FROM Sales WHERE SaleID = ?", 
                parsed_id
            )
            order_result = await cursor.fetchone()
            
            if not order_result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Order '{order_id}' not found."
                )
            
            current_status = order_result.Status.lower()
            if current_status != 'completed':
                raise HTTPException(
                    status_code=400,
                    detail=f"Only completed orders can be refunded. Current status: {current_status}"
                )
            
            # Check refund time window (30 minutes)
            completion_time = order_result.UpdatedAt or order_result.CreatedAt
            
            if completion_time:
                time_since_completion = datetime.now() - completion_time
                if time_since_completion > timedelta(minutes=30):
                    raise HTTPException(
                        status_code=400,
                        detail="Refund window expired. Orders can only be refunded within 30 minutes of completion."
                    )
            
            # Get all items from the order to calculate total refund amount
            await cursor.execute("""
                SELECT 
                    si.SaleItemID,
                    si.ItemName,
                    si.Quantity,
                    si.UnitPrice,
                    COALESCE((
                        SELECT SUM(a.Price * sia.Quantity)
                        FROM SaleItemAddons sia
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = si.SaleItemID
                    ), 0) as AddonTotal
                FROM SaleItems si
                WHERE si.SaleID = ?
            """, parsed_id)
            
            order_items = await cursor.fetchall()
            
            if not order_items:
                raise HTTPException(
                    status_code=400,
                    detail="No items found for this order."
                )
            
            # Calculate total refund amount (items + addons - discount)
            total_refund_amount = Decimal('0.0')
            refund_details = []
            
            for item in order_items:
                item_total = (item.UnitPrice * item.Quantity) + item.AddonTotal
                total_refund_amount += item_total
                
                refund_details.append({
                    'sale_item_id': item.SaleItemID,
                    'item_name': item.ItemName,
                    'quantity': item.Quantity,
                    'refund_amount': float(item_total)
                })
            
            # Subtract discount from total refund
            discount = order_result.TotalDiscountAmount or Decimal('0.0')
            total_refund_amount -= discount
            
            try:
                # Update order status to refunded
                await cursor.execute(
                    "UPDATE Sales SET Status = 'refunded', UpdatedAt = GETDATE() WHERE SaleID = ?", 
                    parsed_id
                )
                
                # Create RefundedOrders record with OUTPUT clause
                await cursor.execute("""
                    INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                    OUTPUT INSERTED.RefundID
                    VALUES (?, ?, ?, GETDATE(), 'full', ?)
                """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
                
                refund_id_row = await cursor.fetchone()
                
                if not refund_id_row:
                    # Fallback method
                    await cursor.execute("""
                        INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                        VALUES (?, ?, ?, GETDATE(), 'full', ?);
                        SELECT @@IDENTITY AS RefundID;
                    """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
                    refund_id_row = await cursor.fetchone()
                
                refund_id = int(refund_id_row[0]) if refund_id_row else None
                
                if not refund_id:
                    await conn.rollback()
                    raise Exception("Failed to create refund record.")
                
                # Insert all items into RefundedItems table
                for detail in refund_details:
                    await cursor.execute("""
                        INSERT INTO RefundedItems (RefundID, SaleItemID, RefundedQuantity, RefundAmount, CreatedAt)
                        VALUES (?, ?, ?, ?, GETDATE())
                    """, refund_id, detail['sale_item_id'], detail['quantity'], detail['refund_amount'])
                
                await conn.commit()
                
                # Get manager's full name for blockchain
                manager_full_name = await get_full_name_from_username(
                    request.managerUsername,
                    current_user['access_token']
                )
                
                # Build detailed item description
                item_descriptions = []
                for detail in refund_details[:3]:
                    item_descriptions.append(f"{detail['quantity']}x {detail['item_name']}")
                
                items_text = " | ".join(item_descriptions)
                if len(refund_details) > 3:
                    items_text += f" +{len(refund_details) - 3} more items"
                
                # Blockchain description (without manager name, frontend adds it)
                blockchain_description = (
                    f"processed a Full Refund: \"{items_text}\" "
                    f"(Total: ₱{float(total_refund_amount):.2f})"
                )
                
                # Blockchain Logging (FULL REFUND) - Cashier is the actor
                cashier_username = current_user.get('username', 'unknown')
                
                refund_data = {
                    "refund_id": refund_id,
                    "refund_type": "full",
                    "total_amount": float(total_refund_amount),
                    "refunded_items": refund_details,
                    "processed_by": cashier_username,
                    "verified_by_manager": request.managerUsername,
                    "manager_full_name": manager_full_name,
                    "reason": request.refundReason
                }
                
                background_tasks.add_task(
                    log_to_blockchain,
                    service_identifier="POS_SALES",
                    action="REFUND",
                    entity_type="SALE",
                    entity_id=parsed_id,
                    actor_username=cashier_username,  # Cashier is the actor
                    change_description=blockchain_description,
                    data=refund_data,
                    token=current_user['access_token']
                )

                logger.info(
                    f"Full refund processed for order {order_id} by cashier {cashier_username}, "
                    f"verified by manager {request.managerUsername}. "
                    f"Refund ID: {refund_id}, Total Amount: {total_refund_amount}, Items: {len(refund_details)}"
                )
                
                return {
                    "message": "Order has been successfully refunded (full refund).",
                    "order_id": order_id,
                    "refund_id": refund_id,
                    "refund_type": "full",
                    "total_refund_amount": float(total_refund_amount),
                    "refunded_items": [
                        {
                            "item_name": detail['item_name'],
                            "quantity": detail['quantity'],
                            "amount": detail['refund_amount']
                        }
                        for detail in refund_details
                    ],
                    "refunded_by": request.managerUsername,
                    "refund_reason": request.refundReason
                }
                
            except Exception as db_exc:
                await conn.rollback()
                logger.error(f"DB error during full refund for order {order_id}: {db_exc}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to process refund in database.")
    
    except HTTPException:
        if conn:
            await conn.rollback()
        raise
    except Exception as e:
        if conn:
            await conn.rollback()
        logger.error(f"Unexpected error during full refund for order {order_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred while processing the refund: {str(e)}"
        )
    
    finally:
        if conn:
            conn.autocommit = True
            await conn.close()


@router_refund.post(
    "/{order_id}/partial-refund",
    status_code=status.HTTP_200_OK,
    summary="Process a partial refund for specific items (within 30 minutes)"
)
async def partial_refund_order(
    order_id: str,
    request: PartialRefundOrderRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Process a partial refund for specific items in a completed order.
    Only allows refunding orders that have status 'completed'.
    Refunds must be processed within 30 minutes of order completion.
    Requires manager authorization.
    CALCULATES NET REFUND AMOUNT AFTER DISCOUNTS AND PROMOTIONS.
    """
    allowed_roles = ["cashier", "admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to process refunds."
        )
    
    try:
        parsed_id = int(order_id.split('-')[-1])
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid order ID format.")
    
    conn = None
    try:
        conn = await get_db_connection()
        conn.autocommit = False
        
        async with conn.cursor() as cursor:
            # Check order exists and is completed
            await cursor.execute(
                "SELECT Status, TotalDiscountAmount, UpdatedAt, CreatedAt FROM Sales WHERE SaleID = ?", 
                parsed_id
            )
            order_result = await cursor.fetchone()
            
            if not order_result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Order '{order_id}' not found."
                )
            
            current_status = order_result.Status.lower()
            if current_status not in ['completed', 'refunded']:
                raise HTTPException(
                    status_code=400,
                    detail=f"Only completed orders can be refunded. Current status: {current_status}"
                )
            
            # Check refund time window (30 minutes)
            completion_time = order_result.UpdatedAt or order_result.CreatedAt
            
            if completion_time:
                time_since_completion = datetime.now() - completion_time
                if time_since_completion > timedelta(minutes=30):
                    raise HTTPException(
                        status_code=400,
                        detail="Refund window expired. Orders can only be refunded within 30 minutes of completion."
                    )
            
            # Validate all items belong to this sale
            sale_item_ids = [item.saleItemId for item in request.items]
            placeholders = ','.join(['?' for _ in sale_item_ids])
            
            await cursor.execute(
                f"SELECT SaleItemID, ItemName, Quantity, UnitPrice FROM SaleItems WHERE SaleID = ? AND SaleItemID IN ({placeholders})",
                parsed_id, *sale_item_ids
            )
            valid_items = await cursor.fetchall()
            
            if len(valid_items) != len(request.items):
                raise HTTPException(
                    status_code=400,
                    detail="One or more items do not belong to this order."
                )
            
            # Check if items were already refunded
            await cursor.execute(f"""
                SELECT ri.SaleItemID, SUM(ri.RefundedQuantity) as TotalRefunded
                FROM RefundedOrders ro
                JOIN RefundedItems ri ON ro.RefundID = ri.RefundID
                WHERE ro.SaleID = ? AND ri.SaleItemID IN ({placeholders})
                GROUP BY ri.SaleItemID
            """, parsed_id, *sale_item_ids)
            
            already_refunded = {row.SaleItemID: row.TotalRefunded for row in await cursor.fetchall()}
            
            # Calculate refund amounts with discounts and promotions
            total_refund_amount = Decimal('0.0')
            refund_details = []
            
            for req_item in request.items:
                matching_item = next((item for item in valid_items if item.SaleItemID == req_item.saleItemId), None)
                
                if not matching_item:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Item {req_item.itemName} not found in order."
                    )
                
                # Check remaining quantity
                original_qty = matching_item.Quantity
                already_refunded_qty = already_refunded.get(req_item.saleItemId, 0)
                remaining_qty = original_qty - already_refunded_qty
                
                if req_item.refundQuantity > remaining_qty:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot refund {req_item.refundQuantity} of {req_item.itemName}. Only {remaining_qty} remaining."
                    )
                
                # CRITICAL: Calculate NET price per unit after discounts/promotions
                unit_price = Decimal(str(matching_item.UnitPrice))
                
                # Get item-level discounts
                await cursor.execute("""
                    SELECT COALESCE(SUM(DiscountAmount), 0) as TotalItemDiscount
                    FROM SaleItemDiscounts
                    WHERE SaleItemID = ?
                """, req_item.saleItemId)
                discount_result = await cursor.fetchone()
                total_item_discount = discount_result.TotalItemDiscount or Decimal('0')
                
                # Get item-level promotions
                await cursor.execute("""
                    SELECT COALESCE(SUM(PromotionAmount), 0) as TotalItemPromotion
                    FROM SaleItemPromotions
                    WHERE SaleItemID = ?
                """, req_item.saleItemId)
                promo_result = await cursor.fetchone()
                total_item_promo = promo_result.TotalItemPromotion or Decimal('0')
                
                # Get addon cost for this item
                await cursor.execute("""
                    SELECT COALESCE(SUM(a.Price * sia.Quantity), 0) as AddonTotal
                    FROM SaleItemAddons sia
                    JOIN Addons a ON sia.AddonID = a.AddonID
                    WHERE sia.SaleItemID = ?
                """, req_item.saleItemId)
                addon_result = await cursor.fetchone()
                addon_total = addon_result.AddonTotal or Decimal('0.0')
                
                # Calculate NET line value
                # Formula: (UnitPrice × Qty) + Addons - ItemDiscounts - ItemPromotions
                line_value = (unit_price * original_qty) + addon_total - total_item_discount - total_item_promo
                
                # Calculate net price per unit
                net_price_per_unit = line_value / original_qty if original_qty > 0 else Decimal('0.0')
                
                # Calculate refund amount for requested quantity
                item_refund_amount = net_price_per_unit * Decimal(str(req_item.refundQuantity))
                
                total_refund_amount += item_refund_amount
                refund_details.append({
                    'sale_item_id': req_item.saleItemId,
                    'refund_quantity': req_item.refundQuantity,
                    'refund_amount': float(item_refund_amount),
                    'item_name': req_item.itemName
                })
            
            # Create refund record
            await cursor.execute("""
                INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                OUTPUT INSERTED.RefundID
                VALUES (?, ?, ?, GETDATE(), 'partial', ?)
            """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
            
            refund_id_row = await cursor.fetchone()
            if not refund_id_row:
                # Fallback method
                await cursor.execute("""
                    INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                    VALUES (?, ?, ?, GETDATE(), 'partial', ?);
                    SELECT @@IDENTITY AS RefundID;
                """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
                refund_id_row = await cursor.fetchone()
            
            refund_id = int(refund_id_row[0]) if refund_id_row else None
            
            if not refund_id:
                await conn.rollback()
                raise Exception("Failed to create refund record.")
            
            # Insert refunded items
            for detail in refund_details:
                await cursor.execute("""
                    INSERT INTO RefundedItems (RefundID, SaleItemID, RefundedQuantity, RefundAmount, CreatedAt)
                    VALUES (?, ?, ?, ?, GETDATE())
                """, refund_id, detail['sale_item_id'], detail['refund_quantity'], detail['refund_amount'])
            
            # Check if all items are fully refunded
            await cursor.execute("""
                SELECT si.SaleItemID, si.Quantity, ISNULL(SUM(ri.RefundedQuantity), 0) as TotalRefunded
                FROM SaleItems si
                LEFT JOIN RefundedItems ri ON si.SaleItemID = ri.SaleItemID
                WHERE si.SaleID = ?
                GROUP BY si.SaleItemID, si.Quantity
            """, parsed_id)

            all_items_check = await cursor.fetchall()
            all_fully_refunded = all(item.Quantity == item.TotalRefunded for item in all_items_check)

            # Update sale status
            if all_fully_refunded:
                await cursor.execute(
                    "UPDATE Sales SET Status = 'refunded', UpdatedAt = GETDATE() WHERE SaleID = ?",
                    parsed_id
                )
                status_message = "All items have been refunded. Order marked as fully refunded."
            else:
                await cursor.execute(
                    "UPDATE Sales SET UpdatedAt = GETDATE() WHERE SaleID = ?",
                    parsed_id
                )
                status_message = "Partial refund processed successfully."
            
            await conn.commit()
            
            # Get manager's full name for blockchain
            manager_full_name = await get_full_name_from_username(
                request.managerUsername,
                current_user['access_token']
            )
            
            # Build detailed item description
            item_descriptions = []
            for detail in refund_details[:3]:
                item_descriptions.append(f"{detail['refund_quantity']}x {detail['item_name']}")
            
            items_text = " | ".join(item_descriptions)
            if len(refund_details) > 3:
                items_text += f" +{len(refund_details) - 3} more items"
            
            blockchain_description = (
                f"processed a Partial Refund: \"{items_text}\" "
                f"(Total: ₱{float(total_refund_amount):.2f})"
            )
            
            cashier_username = current_user.get('username', 'unknown')
            
            refund_data = {
                "refund_id": refund_id,
                "refund_type": "partial",
                "total_amount": float(total_refund_amount),
                "refunded_items": refund_details,
                "processed_by": cashier_username,
                "verified_by_manager": request.managerUsername,
                "manager_full_name": manager_full_name,
                "reason": request.refundReason,
                "all_items_refunded": all_fully_refunded
            }
            
            background_tasks.add_task(
                log_to_blockchain,
                service_identifier="POS_SALES",
                action="REFUND",
                entity_type="SALE",
                entity_id=parsed_id,
                actor_username=cashier_username,
                change_description=blockchain_description,
                data=refund_data,
                token=current_user['access_token']
            )

            logger.info(
                f"Partial refund processed for order {order_id} by cashier {cashier_username}, "
                f"verified by manager {request.managerUsername}. "
                f"Refund ID: {refund_id}, Amount: {total_refund_amount}"
            )
            
            return {
                "message": status_message,
                "order_id": order_id,
                "refund_id": refund_id,
                "refund_type": "full" if all_fully_refunded else "partial",
                "total_refund_amount": float(total_refund_amount),
                "refunded_items": [
                    {
                        "item_name": detail['item_name'],
                        "quantity": detail['refund_quantity'],
                        "amount": detail['refund_amount']
                    }
                    for detail in refund_details
                ],
                "refunded_by": request.managerUsername,
                "refund_reason": request.refundReason
            }
    
    except HTTPException:
        if conn:
            await conn.rollback()
        raise
    except Exception as e:
        if conn:
            await conn.rollback()
        logger.error(f"Unexpected error during partial refund for order {order_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred while processing the partial refund: {str(e)}"
        )
    
    finally:
        if conn:
            conn.autocommit = True
            await conn.close()

@router_refund.get(
    "/{order_id}/refunds",
    status_code=status.HTTP_200_OK,
    summary="Get refund history for a specific order"
)
async def get_order_refunds(
    order_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Retrieve all refunds (full and partial) for a specific order.
    """
    allowed_roles = ["cashier", "admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view refund history."
        )
    
    try:
        parsed_id = int(order_id.split('-')[-1])
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid order ID format.")
    
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT 
                    ro.RefundID,
                    ro.RefundType,
                    ro.RefundAmount,
                    ro.ManagerUsername,
                    ro.RefundReason,
                    ro.RefundedAt,
                    ri.SaleItemID,
                    si.ItemName,
                    ri.RefundedQuantity,
                    ri.RefundAmount as ItemRefundAmount
                FROM RefundedOrders ro
                LEFT JOIN RefundedItems ri ON ro.RefundID = ri.RefundID
                LEFT JOIN SaleItems si ON ri.SaleItemID = si.SaleItemID
                WHERE ro.SaleID = ?
                ORDER BY ro.RefundedAt DESC
            """, parsed_id)
            
            rows = await cursor.fetchall()
            
            if not rows:
                return {
                    "order_id": order_id,
                    "refunds": []
                }
            
            # Group refunds
            refunds_dict = {}
            for row in rows:
                refund_id = row.RefundID
                if refund_id not in refunds_dict:
                    refunds_dict[refund_id] = {
                        "refund_id": refund_id,
                        "refund_type": row.RefundType,
                        "total_amount": float(row.RefundAmount or 0),
                        "manager_username": row.ManagerUsername,
                        "reason": row.RefundReason,
                        "refunded_at": row.RefundedAt.strftime("%B %d, %Y %I:%M %p"),
                        "items": []
                    }
                
                if row.SaleItemID:
                    refunds_dict[refund_id]["items"].append({
                        "item_name": row.ItemName,
                        "quantity": row.RefundedQuantity,
                        "amount": float(row.ItemRefundAmount or 0)
                    })
            
            return {
                "order_id": order_id,
                "refunds": list(refunds_dict.values())
            }
    
    except Exception as e:
        logger.error(f"Error fetching refund history for order {order_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch refund history."
        )
    
    finally:
        if conn:
            await conn.close()


# Same-day refund endpoints (simplified versions)
@router_refund.post(
    "/{order_id}/refund-today",
    status_code=status.HTTP_200_OK,
    summary="Process a full refund (same day only) for a completed order"
)
async def refund_order_today(
    order_id: str,
    request: RefundOrderRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Process a full refund for a completed order.
    Refunds must be processed on the SAME CALENDAR DAY as order completion.
    """
    allowed_roles = ["cashier", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to process refunds."
        )
    
    try:
        parsed_id = int(order_id.split('-')[-1])
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid order ID format.")
    
    conn = None
    try:
        conn = await get_db_connection()
        conn.autocommit = False
        
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT Status, TotalDiscountAmount, UpdatedAt, CreatedAt FROM Sales WHERE SaleID = ?", 
                parsed_id
            )
            order_result = await cursor.fetchone()
            
            if not order_result:
                raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found.")
            
            if order_result.Status.lower() != 'completed':
                raise HTTPException(
                    status_code=400,
                    detail=f"Only completed orders can be refunded. Current status: {order_result.Status}"
                )
            
            # Check if the order was completed today
            completion_time = order_result.UpdatedAt or order_result.CreatedAt
            if completion_time:
                if completion_time.date() != datetime.now().date():
                    raise HTTPException(
                        status_code=400,
                        detail="Refund window expired. Orders can only be refunded on the same day they were completed."
                    )

            await cursor.execute("""
                SELECT si.SaleItemID, si.ItemName, si.Quantity, si.UnitPrice,
                       COALESCE((SELECT SUM(a.Price * sia.Quantity) FROM SaleItemAddons sia JOIN Addons a ON sia.AddonID = a.AddonID WHERE sia.SaleItemID = si.SaleItemID), 0) as AddonTotal
                FROM SaleItems si WHERE si.SaleID = ?
            """, parsed_id)
            order_items = await cursor.fetchall()
            
            if not order_items:
                raise HTTPException(status_code=400, detail="No items found for this order.")

            total_refund_amount = sum((item.UnitPrice * item.Quantity) + item.AddonTotal for item in order_items)
            total_refund_amount -= (order_result.TotalDiscountAmount or Decimal('0.0'))
            
            refund_details = []
            
            try:
                await cursor.execute("UPDATE Sales SET Status = 'refunded', UpdatedAt = GETDATE() WHERE SaleID = ?", parsed_id)
                
                await cursor.execute("""
                    INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                    OUTPUT INSERTED.RefundID
                    VALUES (?, ?, ?, GETDATE(), 'full', ?)
                """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
                refund_id = (await cursor.fetchone())[0]

                for item in order_items:
                    item_total = (item.UnitPrice * item.Quantity) + item.AddonTotal
                    await cursor.execute("""
                        INSERT INTO RefundedItems (RefundID, SaleItemID, RefundedQuantity, RefundAmount, CreatedAt)
                        VALUES (?, ?, ?, ?, GETDATE())
                    """, refund_id, item.SaleItemID, item.Quantity, item_total)
                    
                    refund_details.append({
                        "sale_item_id": item.SaleItemID,
                        "item_name": item.ItemName,
                        "quantity": item.Quantity,
                        "refund_amount": float(item_total)
                    })
                
                await conn.commit()
                
                # Get manager's full name for blockchain
                manager_full_name = await get_full_name_from_username(
                    request.managerUsername,
                    current_user['access_token']
                )
                
                # Build detailed item description
                item_descriptions = []
                for detail in refund_details[:3]:
                    item_descriptions.append(f"{detail['quantity']}x {detail['item_name']}")
                
                items_text = " | ".join(item_descriptions)
                if len(refund_details) > 3:
                    items_text += f" +{len(refund_details) - 3} more items"
                
                blockchain_description = (
                    f"processed a Same-Day Full Refund: \"{items_text}\" "
                    f"(Total: ₱{float(total_refund_amount):.2f})"
                )
                
                # Blockchain Logging (FULL SAME-DAY REFUND)
                refund_data = {
                    "refund_id": refund_id,
                    "refund_type": "full",
                    "refund_window": "same_day",
                    "total_amount": float(total_refund_amount),
                    "refunded_items": refund_details,
                    "manager": request.managerUsername,
                    "manager_full_name": manager_full_name,
                    "reason": request.refundReason
                }
                
                background_tasks.add_task(
                    log_to_blockchain,
                    service_identifier="POS_SALES",
                    action="REFUND",
                    entity_type="SALE",
                    entity_id=parsed_id,
                    actor_username=request.managerUsername,
                    change_description=blockchain_description,
                    data=refund_data,
                    token=current_user['access_token']
                )

                logger.info(f"Full refund (today) processed for order {order_id} by {request.managerUsername}.")
                
                return {
                    "message": "Order has been successfully refunded (full refund).",
                    "order_id": order_id,
                    "refund_id": refund_id,
                    "total_refund_amount": float(total_refund_amount)
                }
                
            except Exception as db_exc:
                await conn.rollback()
                logger.error(f"DB error during full refund (today) for order {order_id}: {db_exc}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to process refund in database.")
    finally:
        if conn:
            conn.autocommit = True
            await conn.close()


@router_refund.post(
    "/{order_id}/partial-refund-today",
    status_code=status.HTTP_200_OK,
    summary="Process a partial refund (same day only) for specific items"
)
async def partial_refund_order_today(
    order_id: str,
    request: PartialRefundOrderRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Process a partial refund for specific items in a completed order.
    Refunds must be processed on the SAME CALENDAR DAY as order completion.
    CALCULATES NET REFUND AMOUNT AFTER DISCOUNTS AND PROMOTIONS.
    """
    allowed_roles = ["cashier", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You do not have permission to process refunds.")
    
    try:
        parsed_id = int(order_id.split('-')[-1])
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid order ID format.")
    
    conn = None
    try:
        conn = await get_db_connection()
        conn.autocommit = False
        
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT Status, UpdatedAt, CreatedAt FROM Sales WHERE SaleID = ?", parsed_id
            )
            order_result = await cursor.fetchone()
            
            if not order_result:
                raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found.")
            
            if order_result.Status.lower() not in ['completed', 'refunded']:
                raise HTTPException(
                    status_code=400,
                    detail=f"Only completed or partially refunded orders can be refunded. Current status: {order_result.Status}"
                )
            
            # Check if the order was completed today
            completion_time = order_result.UpdatedAt or order_result.CreatedAt
            if completion_time:
                if completion_time.date() != datetime.now().date():
                    raise HTTPException(
                        status_code=400,
                        detail="Refund window expired. Orders can only be refunded on the same day they were completed."
                    )

            sale_item_ids = [item.saleItemId for item in request.items]
            placeholders = ','.join(['?' for _ in sale_item_ids])
            
            await cursor.execute(
                f"SELECT SaleItemID, ItemName, Quantity, UnitPrice FROM SaleItems WHERE SaleID = ? AND SaleItemID IN ({placeholders})",
                parsed_id, *sale_item_ids
            )
            valid_items = await cursor.fetchall()
            
            if len(valid_items) != len(request.items):
                raise HTTPException(status_code=400, detail="One or more items do not belong to this order.")
            
            await cursor.execute(f"""
                SELECT ri.SaleItemID, SUM(ri.RefundedQuantity) as TotalRefunded
                FROM RefundedOrders ro JOIN RefundedItems ri ON ro.RefundID = ri.RefundID
                WHERE ro.SaleID = ? AND ri.SaleItemID IN ({placeholders}) GROUP BY ri.SaleItemID
            """, parsed_id, *sale_item_ids)
            already_refunded = {row.SaleItemID: row.TotalRefunded for row in await cursor.fetchall()}
            
            total_refund_amount = Decimal('0.0')
            refund_details = []
            
            for req_item in request.items:
                db_item = next((item for item in valid_items if item.SaleItemID == req_item.saleItemId), None)
                
                if not db_item:
                    raise HTTPException(status_code=400, detail=f"Item {req_item.itemName} not found.")
                
                original_qty = db_item.Quantity
                already_refunded_qty = already_refunded.get(req_item.saleItemId, 0)
                remaining_qty = original_qty - already_refunded_qty
                
                if req_item.refundQuantity > remaining_qty:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot refund {req_item.refundQuantity} of {req_item.itemName}. Only {remaining_qty} remaining."
                    )
                
                # CRITICAL: Calculate NET price per unit after discounts/promotions
                unit_price = Decimal(str(db_item.UnitPrice))
                
                # Get item-level discounts
                await cursor.execute("""
                    SELECT COALESCE(SUM(DiscountAmount), 0) as TotalItemDiscount
                    FROM SaleItemDiscounts
                    WHERE SaleItemID = ?
                """, req_item.saleItemId)
                discount_result = await cursor.fetchone()
                total_item_discount = discount_result.TotalItemDiscount or Decimal('0')
                
                # Get item-level promotions
                await cursor.execute("""
                    SELECT COALESCE(SUM(PromotionAmount), 0) as TotalItemPromotion
                    FROM SaleItemPromotions
                    WHERE SaleItemID = ?
                """, req_item.saleItemId)
                promo_result = await cursor.fetchone()
                total_item_promo = promo_result.TotalItemPromotion or Decimal('0')
                
                # Get addon cost
                await cursor.execute("""
                    SELECT COALESCE(SUM(a.Price * sia.Quantity), 0) as AddonTotal 
                    FROM SaleItemAddons sia 
                    JOIN Addons a ON sia.AddonID = a.AddonID 
                    WHERE sia.SaleItemID = ?
                """, req_item.saleItemId)
                addon_total = (await cursor.fetchone()).AddonTotal or Decimal('0.0')
                
                # Calculate NET line value
                line_value = (unit_price * original_qty) + addon_total - total_item_discount - total_item_promo
                
                # Calculate net price per unit
                net_price_per_unit = line_value / original_qty if original_qty > 0 else Decimal('0.0')
                
                # Calculate refund amount
                item_refund_amount = net_price_per_unit * Decimal(str(req_item.refundQuantity))
                total_refund_amount += item_refund_amount
                
                refund_details.append({
                    'sale_item_id': req_item.saleItemId,
                    'refund_quantity': req_item.refundQuantity,
                    'refund_amount': float(item_refund_amount),
                    'item_name': req_item.itemName
                })
            
            await cursor.execute("""
                INSERT INTO RefundedOrders (SaleID, ManagerUsername, RefundReason, RefundedAt, RefundType, RefundAmount)
                OUTPUT INSERTED.RefundID
                VALUES (?, ?, ?, GETDATE(), 'partial', ?)
            """, parsed_id, request.managerUsername, request.refundReason, total_refund_amount)
            refund_id = (await cursor.fetchone())[0]
            
            for detail in refund_details:
                await cursor.execute("""
                    INSERT INTO RefundedItems (RefundID, SaleItemID, RefundedQuantity, RefundAmount, CreatedAt) 
                    VALUES (?, ?, ?, ?, GETDATE())
                """, refund_id, detail['sale_item_id'], detail['refund_quantity'], detail['refund_amount'])

            await cursor.execute("UPDATE Sales SET UpdatedAt = GETDATE() WHERE SaleID = ?", parsed_id)
            await conn.commit()
            
            manager_full_name = await get_full_name_from_username(
                request.managerUsername,
                current_user['access_token']
            )
            
            item_descriptions = []
            for detail in refund_details[:3]:
                item_descriptions.append(f"{detail['refund_quantity']}x {detail['item_name']}")
            
            items_text = " | ".join(item_descriptions)
            if len(refund_details) > 3:
                items_text += f" +{len(refund_details) - 3} more items"
            
            blockchain_description = (
                f"processed a Same-Day Partial Refund: \"{items_text}\" "
                f"(Total: ₱{float(total_refund_amount):.2f})"
            )
            
            refund_data = {
                "refund_id": refund_id,
                "refund_type": "partial",
                "refund_window": "same_day",
                "total_amount": float(total_refund_amount),
                "refunded_items": refund_details,
                "manager": request.managerUsername,
                "manager_full_name": manager_full_name,
                "reason": request.refundReason
            }
            
            background_tasks.add_task(
                log_to_blockchain,
                service_identifier="POS_SALES",
                action="REFUND",
                entity_type="SALE",
                entity_id=parsed_id,
                actor_username=request.managerUsername,
                change_description=blockchain_description,
                data=refund_data,
                token=current_user['access_token']
            )

            logger.info(f"Partial refund (today) processed for order {order_id} by {request.managerUsername}.")
            
            return {
                "message": "Partial refund processed successfully.",
                "order_id": order_id,
                "refund_id": refund_id,
                "refund_type": "partial",
                "total_refund_amount": float(total_refund_amount),
                "refunded_items": [
                    {
                        "item_name": detail['item_name'],
                        "quantity": detail['refund_quantity'],
                        "amount": detail['refund_amount']
                    }
                    for detail in refund_details
                ],
                "refunded_by": request.managerUsername,
                "refund_reason": request.refundReason
            }

    except HTTPException:
        if conn:
            await conn.rollback()
        raise
    except Exception as e:
        if conn:
            await conn.rollback()
        logger.error(f"Unexpected error during partial refund (today) for order {order_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred while processing the partial refund: {str(e)}"
        )
    
    finally:
        if conn:
            conn.autocommit = True
            await conn.close()