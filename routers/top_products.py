# FILE: top_products.py - COMPLETE WITH REFUND DEDUCTIONS

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from typing import List, Optional
from datetime import date
import sys
import os
import httpx
import logging

# --- Configure logging & DB connection ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_connection

# --- Auth Configuration ---
from fastapi.security import OAuth2PasswordBearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://127.0.0.1:4000/auth/token")
USER_SERVICE_ME_URL = "http://localhost:4000/auth/users/me"

# --- Define the new router ---
router_top_products = APIRouter(
    prefix="/auth/top_products",
    tags=["Top Products"]
)

# --- Authorization Helper ---
async def get_current_active_user(token: str = Depends(oauth2_scheme)):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(USER_SERVICE_ME_URL, headers={"Authorization": f"Bearer {token}"})
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Invalid token: {e.response.text}", headers={"WWW-Authenticate": "Bearer"})
        except httpx.RequestError:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Auth service unavailable.")

# --- Pydantic Models ---
class TopProductsRequest(BaseModel):
    cashierName: str
    orderType: Optional[str] = "All"
    productType: Optional[str] = "All"

class TopProductsByDateRequest(BaseModel):
    cashierName: str
    date: date
    orderType: Optional[str] = "All"
    productType: Optional[str] = "All"

class TopProductItem(BaseModel):
    name: str
    sales: int

# --- Helper to generate WHERE clause for product types ---
def get_product_type_condition(product_type: str) -> str:
    if product_type == "Products":
        return "AND si.Category != 'merchandise'"
    elif product_type == "Merchandise":
        return "AND si.Category = 'merchandise'"
    return ""  # For 'All'

# --- API Endpoint to Get Today's Top Selling Products for a Cashier ---
@router_top_products.post(
    "/today",
    response_model=List[TopProductItem],
    summary="Get today's top selling products for a specific cashier"
)
async def get_top_products_today(
    request: TopProductsRequest,
    current_user: dict = Depends(get_current_active_user)
):
    allowed_roles = ["cashier"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")

    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            # Calculate net quantity sold by subtracting refunded quantities
            base_sql = """
                SELECT TOP 10
                    si.ItemName,
                    SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)) AS NetQuantitySold
                FROM Sales AS s
                JOIN SaleItems AS si ON s.SaleID = si.SaleID
                JOIN CashierSessions AS cs ON s.SessionID = cs.SessionID
                LEFT JOIN (
                    SELECT SaleItemID, SUM(RefundedQuantity) AS TotalRefunded
                    FROM RefundedItems
                    GROUP BY SaleItemID
                ) AS refunds ON si.SaleItemID = refunds.SaleItemID
                WHERE s.Status = 'completed'
                    AND cs.CashierName = ?
                    AND CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
            """
            
            params = [request.cashierName]
            
            if request.orderType == "Store":
                base_sql += " AND s.OrderType IN ('Dine in', 'Take out')"
            elif request.orderType == "Online":
                base_sql += " AND s.OrderType IN ('Pick up', 'Delivery')"
            
            product_type_condition = get_product_type_condition(request.productType)
            base_sql += product_type_condition
            
            base_sql += """
                GROUP BY si.ItemName
                HAVING SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)) > 0
                ORDER BY NetQuantitySold DESC;
            """
            
            await cursor.execute(base_sql, *params)
            rows = await cursor.fetchall()
            
            return [
                TopProductItem(name=row.ItemName, sales=row.NetQuantitySold) 
                for row in rows
            ]
            
    except Exception as e:
        logger.error(f"Error fetching top products for {request.cashierName}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch top selling products.")
    finally:
        if conn:
            await conn.close()

@router_top_products.post(
    "/by_date",
    response_model=List[TopProductItem],
    summary="Get top selling products for a specific cashier by date"
)
async def get_top_products_by_date(
    request: TopProductsByDateRequest,
    current_user: dict = Depends(get_current_active_user)
):
    allowed_roles = ["cashier", "admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")

    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            # Calculate net quantity sold by subtracting refunded quantities
            base_sql = """
                SELECT TOP 10
                    si.ItemName,
                    SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)) AS NetQuantitySold
                FROM Sales AS s
                JOIN SaleItems AS si ON s.SaleID = si.SaleID
                JOIN CashierSessions AS cs ON s.SessionID = cs.SessionID
                LEFT JOIN (
                    SELECT SaleItemID, SUM(RefundedQuantity) AS TotalRefunded
                    FROM RefundedItems
                    GROUP BY SaleItemID
                ) AS refunds ON si.SaleItemID = refunds.SaleItemID
                WHERE s.Status = 'completed'
                    AND cs.CashierName = ?
                    AND CAST(s.CreatedAt AS DATE) = ?
            """
            
            params = [request.cashierName, request.date]
            
            if request.orderType == "Store":
                base_sql += " AND s.OrderType IN ('Dine in', 'Take out')"
            elif request.orderType == "Online":
                base_sql += " AND s.OrderType IN ('Pick up', 'Delivery')"
            
            product_type_condition = get_product_type_condition(request.productType)
            base_sql += product_type_condition
            
            base_sql += """
                GROUP BY si.ItemName
                HAVING SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)) > 0
                ORDER BY NetQuantitySold DESC;
            """
            
            await cursor.execute(base_sql, *params)
            rows = await cursor.fetchall()
            
            top_products = [
                TopProductItem(name=row.ItemName, sales=row.NetQuantitySold)
                for row in rows
            ]
            
            return top_products

    except Exception as e:
        logger.error(f"Error fetching top products for {request.cashierName} on {request.date}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch top selling products."
        )
    finally:
        if conn:
            await conn.close()