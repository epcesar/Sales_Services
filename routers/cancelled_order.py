from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import List, Dict, Optional
from decimal import Decimal
import json
import sys
import os
import httpx
import logging
from datetime import datetime

# --- Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_connection

# --- Auth and Service URL Configuration ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://127.0.0.1:4000/auth/token")
USER_SERVICE_ME_URL = "http://localhost:4000/auth/users/me"

router_cancelled_order = APIRouter(
    prefix="/auth/cancelled_orders",
    tags=["Cancelled Orders"]
)

# --- Authorization Helper Function ---
async def get_current_active_user(token: str = Depends(oauth2_scheme)):
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

# --- Pydantic Models ---
class ProcessingSaleItem(BaseModel):
    name: str
    quantity: int
    price: float
    category: str
    addons: Optional[dict] = {}

class ProcessingOrder(BaseModel):
    id: str
    date: str
    items: int
    total: float
    status: str
    orderType: str
    paymentMethod: str
    cashierName: str
    GCashReferenceNumber: Optional[str] = None
    orderItems: List[ProcessingSaleItem]

class CancelledOrderRequest(BaseModel):
    cashierName: str
    date: str
    orderType: Optional[str] = "All"
    productType: Optional[str] = "All"

# --- Helper functions to generate WHERE clauses ---
def get_order_type_condition(order_type: str) -> str:
    """Generate SQL WHERE clause for order type filtering"""
    if order_type == "Store":
        return "AND s.OrderType IN ('Dine in', 'Take out')"
    elif order_type == "Online":
        return "AND s.OrderType IN ('Pick up', 'Delivery')"
    return ""  # For 'All'

def get_product_type_condition(product_type: str) -> str:
    """Generate SQL WHERE clause for product type filtering"""
    if product_type == "Products":
        return "AND si.Category != 'merchandise'"
    elif product_type == "Merchandise":
        return "AND si.Category = 'merchandise'"
    return ""  # For 'All'

# --- Endpoint to Get Cancelled and Refunded Orders by Date ---
@router_cancelled_order.post(
    "/by_date",
    response_model=List[ProcessingOrder],
    summary="Get Cancelled and Refunded Orders for a Specific Cashier and Date"
)
async def get_cancelled_and_refunded_orders_by_date(
    request: CancelledOrderRequest,
    current_user: dict = Depends(get_current_active_user)
):
    allowed_roles = ["admin", "manager", "cashier"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )

    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:

            # Build the WHERE clause components
            order_type_condition = get_order_type_condition(request.orderType)
            product_type_condition = get_product_type_condition(request.productType)

            # CORRECTED SQL: Join with CashierSessions to get CashierName
            base_sql = f"""
            WITH SaleItemDetails AS (
                SELECT
                    s.SaleID, s.OrderType, s.PaymentMethod, s.CreatedAt, s.UpdatedAt,
                    cs.CashierName, s.GCashReferenceNumber, s.Status,
                    si.SaleItemID, si.ItemName, si.Quantity, si.UnitPrice, si.Category,
                    -- Calculate item subtotal including addons
                    (si.UnitPrice * si.Quantity) + ISNULL((
                        SELECT SUM(a.Price * sia.Quantity)
                        FROM SaleItemAddons sia 
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = si.SaleItemID
                    ), 0) AS ItemTotalWithAddons,
                    -- Get total discount for the entire sale
                    ISNULL(s.TotalDiscountAmount, 0) + ISNULL(s.PromotionalDiscountAmount, 0) AS TotalSaleDiscount
                FROM Sales AS s
                LEFT JOIN CashierSessions AS cs ON s.SessionID = cs.SessionID
                LEFT JOIN SaleItems AS si ON s.SaleID = si.SaleID
                WHERE (
                    s.Status IN ('cancelled', 'refunded')
                    OR EXISTS (
                        SELECT 1 FROM RefundedOrders ro 
                        WHERE ro.SaleID = s.SaleID AND ro.RefundType = 'partial'
                    )
                )
                AND cs.CashierName = ?
                AND CAST(s.UpdatedAt AS DATE) = ?
                {order_type_condition}
                {product_type_condition}
            ),
            -- Calculate sale-level totals for proportional discount distribution
            SaleTotals AS (
                SELECT 
                    SaleID,
                    SUM(ItemTotalWithAddons) AS SaleGrossTotal
                FROM SaleItemDetails
                GROUP BY SaleID
            ),
            -- Get refunded quantities and amounts
            RefundedInfo AS (
                SELECT 
                    ri.SaleItemID,
                    SUM(ri.RefundedQuantity) AS RefundedQty,
                    SUM(ri.RefundAmount) AS RefundedAmount
                FROM RefundedItems ri
                GROUP BY ri.SaleItemID
            ),
            -- Calculate final item values after proportional discounts and refunds
            FinalItems AS (
                SELECT 
                    sid.SaleID, sid.OrderType, sid.PaymentMethod, sid.CreatedAt, sid.UpdatedAt,
                    sid.CashierName, sid.GCashReferenceNumber, sid.Status,
                    sid.SaleItemID, sid.ItemName, sid.Quantity, sid.UnitPrice, sid.Category,
                    -- Apply proportional discount to each item
                    CASE 
                        WHEN st.SaleGrossTotal > 0 
                        THEN sid.ItemTotalWithAddons - ((sid.ItemTotalWithAddons / st.SaleGrossTotal) * sid.TotalSaleDiscount)
                        ELSE sid.ItemTotalWithAddons
                    END AS ItemTotalAfterDiscount,
                    ISNULL(ri.RefundedQty, 0) AS RefundedQty,
                    ISNULL(ri.RefundedAmount, 0) AS RefundedAmount
                FROM SaleItemDetails sid
                JOIN SaleTotals st ON sid.SaleID = st.SaleID
                LEFT JOIN RefundedInfo ri ON sid.SaleItemID = ri.SaleItemID
            ),
            -- Determine refund type for each sale
            RefundTypes AS (
                SELECT 
                    SaleID,
                    MAX(RefundType) AS RefundType
                FROM RefundedOrders
                GROUP BY SaleID
            )
            SELECT 
                fi.SaleID, fi.OrderType, fi.PaymentMethod, fi.CreatedAt, fi.UpdatedAt,
                fi.CashierName, fi.GCashReferenceNumber, fi.Status,
                fi.SaleItemID, fi.ItemName, fi.Quantity, fi.UnitPrice, fi.Category,
                fi.RefundedQty, fi.ItemTotalAfterDiscount, fi.RefundedAmount,
                rt.RefundType
            FROM FinalItems fi
            LEFT JOIN RefundTypes rt ON fi.SaleID = rt.SaleID
            WHERE fi.SaleItemID IS NOT NULL
            ORDER BY fi.UpdatedAt DESC;
            """

            params = [request.cashierName, request.date]
            await cursor.execute(base_sql, *params)
            rows = await cursor.fetchall()

            # Process results
            orders_dict = {}
            
            for row in rows:
                sale_id = row.SaleID
                
                if sale_id not in orders_dict:
                    event_time = row.UpdatedAt or row.CreatedAt
                    orders_dict[sale_id] = {
                        "id": f"SO-{sale_id}",
                        "date": event_time.strftime("%B %d, %Y %I:%M %p"),
                        "status": row.Status,
                        "orderType": row.OrderType,
                        "paymentMethod": row.PaymentMethod,
                        "cashierName": row.CashierName or "Unknown",
                        "GCashReferenceNumber": row.GCashReferenceNumber,
                        "items": 0,
                        "orderItems": [],
                        "_total": Decimal('0.0'),
                        "_refundType": row.RefundType
                    }

                # Determine what to show based on order status
                should_include_item = False
                display_quantity = 0
                item_contribution = Decimal('0.0')

                if row.Status in ('cancelled', 'refunded'):
                    # For fully cancelled/refunded: show original items
                    should_include_item = True
                    display_quantity = row.Quantity
                    item_contribution = row.ItemTotalAfterDiscount
                elif row.Status == 'completed' and row.RefundedQty > 0:
                    # For partial refunds: show only refunded items
                    should_include_item = True
                    display_quantity = row.RefundedQty
                    item_contribution = row.RefundedAmount

                if should_include_item and row.SaleItemID:
                    orders_dict[sale_id]["items"] += display_quantity
                    orders_dict[sale_id]["_total"] += item_contribution

                    # Fetch addons for this item
                    addons_data = {}
                    addons_sql = """
                        SELECT a.AddonName, a.Price, sia.Quantity
                        FROM SaleItemAddons sia
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = ?
                    """
                    await cursor.execute(addons_sql, row.SaleItemID)
                    addon_rows = await cursor.fetchall()

                    for addon_row in addon_rows:
                        addons_data[addon_row.AddonName] = {
                            "price": float(addon_row.Price),
                            "quantity": addon_row.Quantity
                        }

                    orders_dict[sale_id]["orderItems"].append(
                        ProcessingSaleItem(
                            name=row.ItemName,
                            quantity=display_quantity,
                            price=float(row.UnitPrice),
                            category=row.Category,
                            addons=addons_data
                        )
                    )

            # Build response
            response_list = []
            for sale_id, order_data in orders_dict.items():
                if len(order_data["orderItems"]) > 0:
                    # Handle status display
                    if order_data["_refundType"] == 'partial' and order_data["status"] == "completed":
                        order_data["status"] = "partial_refund"
                    
                    order_data["total"] = float(order_data["_total"])
                    del order_data["_total"]
                    del order_data["_refundType"]
                    
                    response_list.append(ProcessingOrder(**order_data))

            return response_list

    except Exception as e:
        logger.error(f"Error fetching cancelled/refunded orders for {request.cashierName} on {request.date}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch cancelled/refunded orders.")
    finally:
        if conn: 
            await conn.close()