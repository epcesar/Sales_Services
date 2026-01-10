# FILE: sales.py - COMPLETE FILE WITH REFUND DEDUCTIONS

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from typing import Optional, Literal, List, Union
from datetime import date, datetime, timedelta
from decimal import Decimal
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
router_sales_metrics = APIRouter(
    prefix="/auth/sales_metrics",
    tags=["Sales Metrics"]
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
class SalesMetricsRequest(BaseModel):
    cashierName: str
    orderType: Optional[Literal['All', 'Store', 'Online']] = 'All'
    productType: Optional[Literal['All', 'Products', 'Merchandise']] = 'All'

class SalesMetricsByDateRequest(BaseModel):
    cashierName: str
    date: date
    orderType: Optional[Literal['All', 'Store', 'Online']] = 'All'
    productType: Optional[Literal['All', 'Products', 'Merchandise']] = 'All'

class SalesMetricsResponse(BaseModel):
    totalSales: float
    cashSales: float
    gcashSales: float
    itemsSold: int

class DailyReportItem(BaseModel):
    productName: str
    category: Optional[str]
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float

class WeeklyReportItem(BaseModel):
    day: str
    transactions: int
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float
    bestItem: Optional[str]

class MonthlyReportItem(BaseModel):
    week: str
    period: str
    transactions: int
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float
    bestItem: Optional[str]

class YearlyReportItem(BaseModel):
    month: str
    transactions: int
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float
    bestItem: Optional[str]

class CustomReportItem(BaseModel):
    date: date
    transactions: int
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float
    bestItem: Optional[str]

class SalesReportRequest(BaseModel):
    reportType: Literal['daily', 'weekly', 'monthly', 'yearly', 'custom']
    startDate: Optional[date] = None
    endDate: Optional[date] = None

class Totals(BaseModel):
    transactions: int
    itemsSold: int
    storeSale: float
    onlineSale: float
    totalSale: float

class SalesReportResponse(BaseModel):
    data: List[Union[DailyReportItem, WeeklyReportItem, MonthlyReportItem, YearlyReportItem, CustomReportItem]]
    totals: Totals

# --- Pydantic model for the INCOMING REQUEST ---
class AlignedSalesReportRequest(BaseModel):
     reportType: Literal['daily', 'weekly', 'monthly', 'quarterly', 'custom'] # FIXED: Align with frontend
     startDate: Optional[date] = None
     endDate: Optional[date] = None
     cashierName: Optional[str] = None


# --- Pydantic Models for the OUTGOING RESPONSE ---
class SalesReportSummary(BaseModel):
    totalSales: float = 0.0
    cashInDrawer: float = 0.0
    discrepancy: float = 0.0
    transactions: int = 0
    refunds: float = 0.0
    
class PaymentSummary(BaseModel):
    cashAmount: float = 0.0
    gcashAmount: float = 0.0
    
class CategoryBreakdownItem(BaseModel):
    category: str
    quantity: int
    sales: float

class ProductBreakdownItem(BaseModel):
    product: str
    category: str
    units: int
    total: float

class CashDrawerSummary(BaseModel):
    opening: float = 0.0
    cashSales: float = 0.0
    refunds: float = 0.0
    expected: float = 0.0
    actual: float = 0.0
    discrepancy: float = 0.0
    reportedBy: str = "N/A"
    verifiedBy: str = "N/A"

class PaymentMethodItem(BaseModel):
    type: str
    transactions: int
    amount: float

class RefundItem(BaseModel):
    id: str
    product: str
    amount: float
    reason: Optional[str] = "N/A"
    cashier: str
    date: str

class AlignedSalesReportResponse(BaseModel):
    summary: SalesReportSummary
    paymentSummary: PaymentSummary
    categoryBreakdown: List[CategoryBreakdownItem]
    productBreakdown: List[ProductBreakdownItem]
    cashDrawer: CashDrawerSummary
    paymentMethods: List[PaymentMethodItem]
    refundsList: List[RefundItem]

class SalesMonitoringRequest(BaseModel):
    dateRange: Literal['today', 'week', 'month', 'custom'] 
    selectedCashier: Optional[str] = 'all'
    selectedCategory: Optional[str] = 'all'
    startDate: Optional[date] = None
    endDate: Optional[date] = None

class ProductSalesDetail(BaseModel):
    id: int
    product: str
    category: str
    revenue: float
    profit: float
    quantity: int
    date: str
    cashier: Optional[str]

class SalesMonitoringResponse(BaseModel):
    salesData: List[ProductSalesDetail]
    totalRevenue: float
    totalProfit: float
    totalQuantity: int
    profitMargin: float
    transactionCount: int

# --- Helper to generate WHERE clause for order types ---
def get_order_type_condition(order_type: str) -> str:
    if order_type == 'Store':
        return "AND s.OrderType IN ('Dine In', 'Take Out')"
    elif order_type == 'Online':
        return "AND s.OrderType IN ('Pick Up', 'Delivery')"
    return "" # For 'All'

# --- Helper to generate WHERE clause for product types ---
def get_product_type_condition(product_type: str) -> str:
    if product_type == 'Products':
        return "AND si.Category != 'merchandise'"
    elif product_type == 'Merchandise':
        return "AND si.Category = 'merchandise'"
    return ""


@router_sales_metrics.post(
    "/current_session",
    response_model=SalesMetricsResponse,
    summary="Get Sales Metrics for the Current Active Session"
)
async def get_current_session_sales_metrics(
    request: SalesMetricsRequest,
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["cashier"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT SessionStart, SessionID 
                FROM CashierSessions 
                WHERE CashierName = ? AND Status = 'Active'
            """, request.cashierName)
            session_row = await cursor.fetchone()
            if not session_row:
                return SalesMetricsResponse(totalSales=0.0, cashSales=0.0, gcashSales=0.0, itemsSold=0)
            session_start_time = session_row.SessionStart
            session_id = session_row.SessionID
            order_type_condition = get_order_type_condition(request.orderType)
            product_type_condition = get_product_type_condition(request.productType)
            
            # Query 1: Count Net items sold (Original - Refunded)
            count_sql = f"""
                SELECT CAST(ISNULL(SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)), 0) AS INT) AS ItemsSold
                FROM Sales s
                JOIN SaleItems si ON s.SaleID = si.SaleID
                LEFT JOIN (
                    SELECT SaleItemID, SUM(RefundedQuantity) as TotalRefunded
                    FROM RefundedItems
                    GROUP BY SaleItemID
                ) AS refunds ON si.SaleItemID = refunds.SaleItemID
                WHERE s.Status = 'completed'
                    AND s.SessionID = ?
                    AND s.CreatedAt >= ?
                    {order_type_condition}
                    {product_type_condition}
            """
            
            await cursor.execute(count_sql, (session_id, session_start_time))
            count_row = await cursor.fetchone()
            items_sold = count_row.ItemsSold if count_row else 0
            
            # Query 2: Calculate sales amounts (with refund deductions)
            sales_sql = f"""
                WITH SaleItemDetails AS (
                    SELECT 
                        s.SaleID,
                        s.PaymentMethod,
                        si.SaleItemID,
                        si.Quantity AS OriginalQuantity,
                        si.UnitPrice,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    JOIN SaleItems si ON s.SaleID = si.SaleID
                    WHERE s.Status = 'completed' 
                        AND s.SessionID = ? 
                        AND s.CreatedAt >= ?
                        {order_type_condition}
                        {product_type_condition}
                ),
                ItemLevelDiscounts AS (
                    SELECT 
                        sid.SaleItemID,
                        SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    JOIN SaleItemDetails sid2 ON sid.SaleItemID = sid2.SaleItemID
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT 
                        sip.SaleItemID,
                        SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    JOIN SaleItemDetails sid ON sip.SaleItemID = sid.SaleItemID
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT 
                        ri.SaleItemID,
                        SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    JOIN SaleItemDetails sid ON ri.SaleItemID = sid.SaleItemID
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.PaymentMethod,
                        (sid.ItemSubtotal + sid.AddonsTotal 
                         - ISNULL(ild.TotalItemDiscount, 0) 
                         - ISNULL(ilp.TotalItemPromotion, 0)) 
                        - ISNULL(ri.TotalRefundAmount, 0) AS NetAmount
                    FROM SaleItemDetails sid
                    LEFT JOIN ItemLevelDiscounts ild ON sid.SaleItemID = ild.SaleItemID
                    LEFT JOIN ItemLevelPromotions ilp ON sid.SaleItemID = ilp.SaleItemID
                    LEFT JOIN RefundedItemsDetail ri ON sid.SaleItemID = ri.SaleItemID
                    WHERE (sid.ItemSubtotal + sid.AddonsTotal 
                           - ISNULL(ild.TotalItemDiscount, 0) 
                           - ISNULL(ilp.TotalItemPromotion, 0) 
                           - ISNULL(ri.TotalRefundAmount, 0)) > 0.01
                )
                SELECT 
                    ISNULL(SUM(NetAmount), 0) AS TotalSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod = 'Cash' THEN NetAmount ELSE 0 END), 0) AS CashSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN NetAmount ELSE 0 END), 0) AS GcashSales
                FROM NetSaleItems
            """
            
            await cursor.execute(sales_sql, (session_id, session_start_time))
            sales_row = await cursor.fetchone()
            
            if sales_row:
                return SalesMetricsResponse(
                    totalSales=float(sales_row.TotalSales), 
                    cashSales=float(sales_row.CashSales), 
                    gcashSales=float(sales_row.GcashSales), 
                    itemsSold=items_sold
                )
            return SalesMetricsResponse(
                totalSales=0.0, 
                cashSales=0.0, 
                gcashSales=0.0, 
                itemsSold=items_sold
            )
    except Exception as e:
        logger.error(f"Error fetching current session metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch sales metrics.")
    finally:
        if conn: await conn.close()

@router_sales_metrics.post(
    "/today",
    response_model=SalesMetricsResponse,
    summary="Get ALL of Today's Sales Metrics for a Specific Cashier"
)
async def get_todays_sales_metrics(
    request: SalesMetricsRequest,
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["cashier"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            order_type_condition = get_order_type_condition(request.orderType)
            product_type_condition = get_product_type_condition(request.productType)
            
            # Query 1: Count Net items sold (Original - Refunded)
            count_sql = f"""
                SELECT CAST(ISNULL(SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)), 0) AS INT) AS ItemsSold
                FROM Sales s
                JOIN SaleItems si ON s.SaleID = si.SaleID
                JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                LEFT JOIN (
                    SELECT SaleItemID, SUM(RefundedQuantity) as TotalRefunded
                    FROM RefundedItems
                    GROUP BY SaleItemID
                ) AS refunds ON si.SaleItemID = refunds.SaleItemID
                WHERE s.Status = 'completed'
                    AND cs.CashierName = ?
                    AND CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
                    {order_type_condition}
                    {product_type_condition}
            """
            
            await cursor.execute(count_sql, (request.cashierName,))
            count_row = await cursor.fetchone()
            items_sold = count_row.ItemsSold if count_row else 0
            
            # Query 2: Calculate sales amounts
            sales_sql = f"""
                WITH SaleItemDetails AS (
                    SELECT 
                        s.SaleID,
                        s.PaymentMethod,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s 
                    JOIN SaleItems si ON s.SaleID = si.SaleID
                    JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                    WHERE s.Status = 'completed' 
                        AND cs.CashierName = ? 
                        AND CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
                        {order_type_condition}
                        {product_type_condition}
                ),
                ItemLevelDiscounts AS (
                    SELECT 
                        sid.SaleItemID,
                        SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sid.SaleItemID
                    )
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT 
                        sip.SaleItemID,
                        SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sip.SaleItemID
                    )
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT 
                        ri.SaleItemID,
                        SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = ri.SaleItemID
                    )
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.PaymentMethod,
                        (sid.ItemSubtotal + sid.AddonsTotal 
                         - ISNULL(ild.TotalItemDiscount, 0) 
                         - ISNULL(ilp.TotalItemPromotion, 0)) 
                        - ISNULL(ri.TotalRefundAmount, 0) AS NetAmount
                    FROM SaleItemDetails sid
                    LEFT JOIN ItemLevelDiscounts ild ON sid.SaleItemID = ild.SaleItemID
                    LEFT JOIN ItemLevelPromotions ilp ON sid.SaleItemID = ilp.SaleItemID
                    LEFT JOIN RefundedItemsDetail ri ON sid.SaleItemID = ri.SaleItemID
                    WHERE (sid.ItemSubtotal + sid.AddonsTotal 
                           - ISNULL(ild.TotalItemDiscount, 0) 
                           - ISNULL(ilp.TotalItemPromotion, 0) 
                           - ISNULL(ri.TotalRefundAmount, 0)) > 0.01
                )
                SELECT 
                    ISNULL(SUM(NetAmount), 0) AS TotalSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod = 'Cash' THEN NetAmount ELSE 0 END), 0) AS CashSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN NetAmount ELSE 0 END), 0) AS GcashSales
                FROM NetSaleItems
            """
            
            await cursor.execute(sales_sql, (request.cashierName,))
            sales_row = await cursor.fetchone()
            
            if sales_row:
                return SalesMetricsResponse(
                    totalSales=float(sales_row.TotalSales), 
                    cashSales=float(sales_row.CashSales), 
                    gcashSales=float(sales_row.GcashSales), 
                    itemsSold=items_sold
                )
            return SalesMetricsResponse(
                totalSales=0.0, 
                cashSales=0.0, 
                gcashSales=0.0, 
                itemsSold=items_sold
            )
    except Exception as e:
        logger.error(f"Error fetching today's sales metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch sales metrics.")
    finally:
        if conn: await conn.close()

@router_sales_metrics.post(
    "/by_date",
    response_model=SalesMetricsResponse,
    summary="Get Sales Metrics for a Specific Cashier by Date"
)
async def get_sales_metrics_by_date(
    request: SalesMetricsByDateRequest,
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["cashier", "admin", "manager"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")

    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            order_type_condition = get_order_type_condition(request.orderType)
            product_type_condition = get_product_type_condition(request.productType)

            # Query 1: Count Net items sold (Original - Refunded)
            count_sql = f"""
                SELECT CAST(ISNULL(SUM(si.Quantity - ISNULL(refunds.TotalRefunded, 0)), 0) AS INT) AS ItemsSold
                FROM Sales s
                JOIN SaleItems si ON s.SaleID = si.SaleID
                JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                LEFT JOIN (
                    SELECT SaleItemID, SUM(RefundedQuantity) as TotalRefunded
                    FROM RefundedItems
                    GROUP BY SaleItemID
                ) AS refunds ON si.SaleItemID = refunds.SaleItemID
                WHERE s.Status = 'completed'
                    AND cs.CashierName = ?
                    AND CAST(s.CreatedAt AS DATE) = ?
                    {order_type_condition}
                    {product_type_condition}
            """
            
            await cursor.execute(count_sql, (request.cashierName, request.date))
            count_row = await cursor.fetchone()
            items_sold = count_row.ItemsSold if count_row else 0

            # Query 2: Calculate sales amounts
            sales_sql = f"""
                WITH SaleItemDetails AS (
                    SELECT 
                        s.SaleID,
                        s.PaymentMethod,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    JOIN SaleItems si ON s.SaleID = si.SaleID
                    JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                    WHERE s.Status = 'completed' 
                        AND cs.CashierName = ? 
                        AND CAST(s.CreatedAt AS DATE) = ?
                        {order_type_condition}
                        {product_type_condition}
                ),
                ItemLevelDiscounts AS (
                    SELECT 
                        sid.SaleItemID,
                        SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sid.SaleItemID
                    )
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT 
                        sip.SaleItemID,
                        SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sip.SaleItemID
                    )
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT 
                        ri.SaleItemID,
                        SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = ri.SaleItemID
                    )
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.PaymentMethod,
                        (sid.ItemSubtotal + sid.AddonsTotal 
                         - ISNULL(ild.TotalItemDiscount, 0) 
                         - ISNULL(ilp.TotalItemPromotion, 0)) 
                        - ISNULL(ri.TotalRefundAmount, 0) AS NetAmount
                    FROM SaleItemDetails sid
                    LEFT JOIN ItemLevelDiscounts ild ON sid.SaleItemID = ild.SaleItemID
                    LEFT JOIN ItemLevelPromotions ilp ON sid.SaleItemID = ilp.SaleItemID
                    LEFT JOIN RefundedItemsDetail ri ON sid.SaleItemID = ri.SaleItemID
                    WHERE (sid.ItemSubtotal + sid.AddonsTotal 
                           - ISNULL(ild.TotalItemDiscount, 0) 
                           - ISNULL(ilp.TotalItemPromotion, 0) 
                           - ISNULL(ri.TotalRefundAmount, 0)) > 0.01
                )
                SELECT 
                    ISNULL(SUM(NetAmount), 0) AS TotalSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod = 'Cash' THEN NetAmount ELSE 0 END), 0) AS CashSales,
                    ISNULL(SUM(CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN NetAmount ELSE 0 END), 0) AS GcashSales
                FROM NetSaleItems
            """
            
            await cursor.execute(sales_sql, (request.cashierName, request.date))
            sales_row = await cursor.fetchone()
            
            if sales_row:
                return SalesMetricsResponse(
                    totalSales=float(sales_row.TotalSales), 
                    cashSales=float(sales_row.CashSales), 
                    gcashSales=float(sales_row.GcashSales), 
                    itemsSold=items_sold
                )
            return SalesMetricsResponse(
                totalSales=0.0, 
                cashSales=0.0, 
                gcashSales=0.0, 
                itemsSold=items_sold
            )
    except Exception as e:
        logger.error(f"Error fetching sales metrics for date {request.date}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch sales metrics by date.")
    finally:
        if conn: await conn.close()

# FILE: sales.py (~Line 553)

@router_sales_metrics.post(
    "/report",
    response_model=AlignedSalesReportResponse,
    summary="Generate a comprehensive sales report for a specific period"
)
async def get_sales_report(
    request: AlignedSalesReportRequest, 
    current_user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    if current_user.get("userRole") not in ["admin", "manager"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")

    start_date, end_date = None, None
    today = date.today() # Use standard Python date objects for calculation

    # FIXED: Logic updated to handle 'daily', 'weekly', 'monthly', 'quarterly'
    if request.reportType == 'daily':
        start_date = end_date = today
    elif request.reportType == 'weekly':
        # Calculate start of the week (assuming Monday start, weekday() is 0=Mon, 6=Sun)
        days_to_subtract = today.weekday()
        start_date = today - timedelta(days=days_to_subtract)
        end_date = today
    elif request.reportType == 'monthly':
        # Start of the month
        start_date = today.replace(day=1)
        end_date = today
    elif request.reportType == 'quarterly':
        # Calculate the start of the current quarter (Jan, Apr, Jul, Oct)
        current_month = today.month
        quarter_start_month = 3 * ((current_month - 1) // 3) + 1
        start_date = today.replace(month=quarter_start_month, day=1)
        end_date = today
    elif request.reportType == 'custom':
        start_date, end_date = request.startDate, request.endDate

    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="A valid date range is required for the 'custom' report type or could not be determined.")

    # Build cashier filter condition
    cashier_condition = ""
    cashier_params = []
    if request.cashierName and request.cashierName.lower() != "all":
        cashier_condition = "AND cs.CashierName = ?"
        cashier_params.append(request.cashierName)

    # NEW IMPROVED CTE - Same logic as dashboard.py
    base_query_cte = f"""
        WITH SaleItemDetails AS (
            SELECT 
                s.SaleID, s.OrderType, s.CreatedAt, s.PaymentMethod, cs.CashierName,
                si.SaleItemID, si.ItemName, si.Category, si.Quantity,
                (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                ISNULL((
                    SELECT SUM(a.Price * sia.Quantity) 
                    FROM SaleItemAddons sia 
                    JOIN Addons a ON sia.AddonID = a.AddonID 
                    WHERE sia.SaleItemID = si.SaleItemID
                ), 0) AS AddonsTotal
            FROM Sales s 
            JOIN SaleItems si ON s.SaleID = si.SaleID 
            JOIN CashierSessions cs ON s.SessionID = cs.SessionID
            WHERE s.Status = 'completed' 
                AND CAST(s.CreatedAt AS DATE) BETWEEN ? AND ?
                {cashier_condition}
        ),
        ItemLevelDiscounts AS (
            SELECT 
                sid.SaleItemID,
                SUM(sid.DiscountAmount) AS TotalItemDiscount
            FROM SaleItemDiscounts sid
            WHERE EXISTS (
                SELECT 1 FROM SaleItemDetails s2 
                WHERE s2.SaleItemID = sid.SaleItemID
            )
            GROUP BY sid.SaleItemID
        ),
        ItemLevelPromotions AS (
            SELECT 
                sip.SaleItemID,
                SUM(sip.PromotionAmount) AS TotalItemPromotion
            FROM SaleItemPromotions sip
            WHERE EXISTS (
                SELECT 1 FROM SaleItemDetails s2 
                WHERE s2.SaleItemID = sip.SaleItemID
            )
            GROUP BY sip.SaleItemID
        ),
        RefundedAmounts AS (
            SELECT 
                ri.SaleItemID,
                SUM(ri.RefundAmount) AS TotalRefunded,
                SUM(ri.RefundedQuantity) AS RefundedQty
            FROM RefundedItems ri
            WHERE EXISTS (
                SELECT 1 FROM SaleItemDetails s2 
                WHERE s2.SaleItemID = ri.SaleItemID
            )
            GROUP BY ri.SaleItemID
        ),
        FinalSales AS (
            SELECT 
                sit.SaleID, sit.OrderType, sit.CreatedAt, sit.PaymentMethod, sit.CashierName, 
                sit.SaleItemID, sit.ItemName, sit.Category, 
                sit.Quantity - ISNULL(ra.RefundedQty, 0) AS NetQuantity,
                (sit.ItemSubtotal + sit.AddonsTotal 
                 - ISNULL(ild.TotalItemDiscount, 0) 
                 - ISNULL(ilp.TotalItemPromotion, 0)) 
                - ISNULL(ra.TotalRefunded, 0) as LineTotal
            FROM SaleItemDetails sit
            LEFT JOIN ItemLevelDiscounts ild ON sit.SaleItemID = ild.SaleItemID
            LEFT JOIN ItemLevelPromotions ilp ON sit.SaleItemID = ilp.SaleItemID
            LEFT JOIN RefundedAmounts ra ON sit.SaleItemID = ra.SaleItemID
            WHERE ((sit.ItemSubtotal + sit.AddonsTotal 
                   - ISNULL(ild.TotalItemDiscount, 0) 
                   - ISNULL(ilp.TotalItemPromotion, 0)) 
                   - ISNULL(ra.TotalRefunded, 0)) > 0.01
        )
    """
    
    params = [start_date, end_date] + cashier_params
    conn = None
    
    try:
        conn = await get_db_connection()
        
        # 1. Fetch Summary Data
        summary_query = base_query_cte + """
            SELECT 
                ISNULL(SUM(LineTotal), 0) as totalSales,
                COUNT(DISTINCT SaleID) as transactions,
                ISNULL(SUM(CASE WHEN PaymentMethod = 'Cash' THEN LineTotal ELSE 0 END), 0) as cashAmount,
                ISNULL(SUM(CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN LineTotal ELSE 0 END), 0) as gcashAmount
            FROM FinalSales;
        """
        async with conn.cursor() as cursor:
            await cursor.execute(summary_query, tuple(params))
            columns = [column[0] for column in cursor.description]
            summary_row = await cursor.fetchone()
            summary_row_dict = dict(zip(columns, summary_row)) if summary_row else {}

        # 2. Fetch Category Breakdown
        category_query = base_query_cte + """
            SELECT Category AS category, SUM(NetQuantity) AS quantity, SUM(LineTotal) AS sales 
            FROM FinalSales 
            GROUP BY Category 
            ORDER BY sales DESC;
        """
        async with conn.cursor() as cursor:
            await cursor.execute(category_query, tuple(params))
            columns = [column[0] for column in cursor.description]
            category_rows = await cursor.fetchall()
            category_breakdown = [dict(zip(columns, row)) for row in category_rows]

        # 3. Fetch Product Breakdown
        product_query = base_query_cte + """
            SELECT ItemName AS product, Category AS category, SUM(NetQuantity) AS units, SUM(LineTotal) AS total 
            FROM FinalSales 
            GROUP BY ItemName, Category 
            ORDER BY total DESC;
        """
        async with conn.cursor() as cursor:
            await cursor.execute(product_query, tuple(params))
            columns = [column[0] for column in cursor.description]
            product_rows = await cursor.fetchall()
            product_breakdown = [dict(zip(columns, row)) for row in product_rows]

        # 4. Fetch Payment Methods Breakdown
        payment_methods_query = base_query_cte + """
            SELECT 
                CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN 'GCash' ELSE PaymentMethod END as type, 
                COUNT(DISTINCT SaleID) as transactions, 
                SUM(LineTotal) as amount 
            FROM FinalSales 
            GROUP BY CASE WHEN PaymentMethod IN ('GCash', 'E-Wallet') THEN 'GCash' ELSE PaymentMethod END;
        """
        async with conn.cursor() as cursor:
            await cursor.execute(payment_methods_query, tuple(params))
            columns = [column[0] for column in cursor.description]
            payment_rows = await cursor.fetchall()
            payment_methods = [dict(zip(columns, row)) for row in payment_rows]

        # 5. Fetch Refunds List (with corrected amount calculation)
        refunds_params = [start_date, end_date]
        cashier_condition_refunds = ""
        if request.cashierName and request.cashierName.lower() != "all":
            cashier_condition_refunds = "AND cs.CashierName = ?"
            refunds_params.append(request.cashierName)
            
        refunds_query = f"""
            WITH CorrectRefundAmount AS (
                SELECT 
                    ro.SaleID,
                    ro.RefundReason,
                    ro.RefundedAt,
                    cs.CashierName,
                    si.ItemName,
                    ri.RefundedQuantity,
                    CASE 
                        WHEN si.Quantity = 0 THEN 0
                        ELSE 
                            (
                                -- Calculate the net total for the original sale item line
                                (si.UnitPrice * si.Quantity) + 
                                ISNULL((SELECT SUM(a.Price * sia.Quantity) FROM SaleItemAddons sia JOIN Addons a ON sia.AddonID = a.AddonID WHERE sia.SaleItemID = si.SaleItemID), 0) - 
                                ISNULL((SELECT SUM(sid.DiscountAmount) FROM SaleItemDiscounts sid WHERE sid.SaleItemID = si.SaleItemID), 0) -
                                ISNULL((SELECT SUM(sip.PromotionAmount) FROM SaleItemPromotions sip WHERE sip.SaleItemID = si.SaleItemID), 0)
                            ) / si.Quantity -- Get the net price per unit
                    END * ri.RefundedQuantity AS CalculatedRefundAmount -- Multiply by refunded quantity
                FROM RefundedOrders ro
                JOIN Sales s ON ro.SaleID = s.SaleID
                JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                JOIN RefundedItems ri ON ro.RefundID = ri.RefundID
                JOIN SaleItems si ON ri.SaleItemID = si.SaleItemID
                WHERE CAST(ro.RefundedAt AS DATE) BETWEEN ? AND ?
                {cashier_condition_refunds}
            )
            SELECT 
                CAST(SaleID AS VARCHAR) as id,
                ItemName as product,
                CalculatedRefundAmount AS amount,
                RefundReason as reason,
                CashierName as cashier,
                FORMAT(RefundedAt, 'MM/dd/yyyy') as date
            FROM CorrectRefundAmount
            WHERE CalculatedRefundAmount > 0;
        """
        
        async with conn.cursor() as cursor:
            await cursor.execute(refunds_query, tuple(refunds_params))
            columns = [column[0] for column in cursor.description]
            refund_rows = await cursor.fetchall()
            refunds_list = [dict(zip(columns, row)) for row in refund_rows]
            total_refund_amount = sum(float(item.get('amount', 0.0)) for item in refunds_list)

        # 6. Fetch Cash Drawer Summary
        cash_drawer_base_condition = "WHERE cs.Status = 'Closed' AND CAST(cs.SessionEnd AS DATE) BETWEEN ? AND ?"
        cash_drawer_params = [start_date, end_date]
        
        if request.cashierName and request.cashierName.lower() != "all":
            cash_drawer_base_condition += " AND cs.CashierName = ?"
            cash_drawer_params.append(request.cashierName)
        
        cash_drawer_query = f"""
            WITH SessionRefunds AS (
                SELECT 
                    cs.SessionID,
                    cs.InitialCash,
                    cs.CashSalesAtClose,
                    cs.ClosingCash,
                    cs.CashierName,
                    cs.SessionStart,
                    cs.SessionEnd,
                    cs.CheckedBy,
                    ISNULL(cd.DiscrepancyAmount, 0) as DiscrepancyAmount,
                    ISNULL((
                        SELECT SUM(ri.RefundAmount)
                        FROM RefundedOrders ro
                        JOIN RefundedItems ri ON ro.RefundID = ri.RefundID
                        JOIN Sales s ON ro.SaleID = s.SaleID
                        WHERE s.PaymentMethod = 'Cash'
                            AND s.SessionID = cs.SessionID
                            AND ro.RefundedAt BETWEEN cs.SessionStart AND ISNULL(cs.SessionEnd, GETDATE())
                    ), 0) AS TotalRefunds
                FROM CashierSessions cs
                LEFT JOIN CashDiscrepancies cd ON cs.SessionID = cd.SessionID
                {cash_drawer_base_condition}
            )
            SELECT TOP 1
                InitialCash as opening,
                CashSalesAtClose as grossCashSales,
                TotalRefunds as refunds,
                ClosingCash as actual,
                CashierName as reportedBy,
                CheckedBy as verifiedBy,
                DiscrepancyAmount as discrepancy
            FROM SessionRefunds
            ORDER BY SessionEnd DESC;
        """
        
        async with conn.cursor() as cursor:
            await cursor.execute(cash_drawer_query, tuple(cash_drawer_params))
            columns = [column[0] for column in cursor.description]
            cash_drawer_row = await cursor.fetchone()
            cash_drawer_row_dict = dict(zip(columns, cash_drawer_row)) if cash_drawer_row else {}
        
        # Fetch full names for reportedBy and verifiedBy
        reported_by_name = "N/A"
        verified_by_name = "N/A"
        
        if cash_drawer_row_dict:
            reported_by_username = cash_drawer_row_dict.get('reportedBy')
            verified_by_username = cash_drawer_row_dict.get('verifiedBy')
            
            if reported_by_username:
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(
                            f"http://localhost:4000/users/employee_name?username={reported_by_username}",
                            headers={"Authorization": f"Bearer {token}"}
                        )
                        if response.status_code == 200:
                            data = response.json()
                            reported_by_name = data.get('employee_name', reported_by_username)
                        else:
                            reported_by_name = reported_by_username
                except Exception as e:
                    logger.warning(f"Could not fetch employee name for {reported_by_username}: {e}")
                    reported_by_name = reported_by_username
            
            if verified_by_username:
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(
                            f"http://localhost:4000/users/employee_name?username={verified_by_username}",
                            headers={"Authorization": f"Bearer {token}"}
                        )
                        if response.status_code == 200:
                            data = response.json()
                            verified_by_name = data.get('employee_name', verified_by_username)
                        else:
                            verified_by_name = verified_by_username
                except Exception as e:
                    logger.warning(f"Could not fetch employee name for {verified_by_username}: {e}")
                    verified_by_name = verified_by_username
        
        cash_drawer = CashDrawerSummary()
        if cash_drawer_row_dict:
            opening_bal = float(cash_drawer_row_dict.get('opening') or 0.0)
            gross_cash_sales = float(cash_drawer_row_dict.get('grossCashSales') or 0.0)
            total_refunds = float(cash_drawer_row_dict.get('refunds') or 0.0)
            actual = float(cash_drawer_row_dict.get('actual') or 0.0)
            stored_discrepancy = float(cash_drawer_row_dict.get('discrepancy') or 0.0)
            
            net_cash_sales = gross_cash_sales - total_refunds
            expected = opening_bal + net_cash_sales
            
            cash_drawer = CashDrawerSummary(
                opening=opening_bal,
                cashSales=net_cash_sales,
                refunds=total_refunds,
                expected=expected,
                actual=actual,
                discrepancy=stored_discrepancy,
                reportedBy=reported_by_name,
                verifiedBy=verified_by_name
            )

        summary = SalesReportSummary(
            totalSales=float(summary_row_dict.get('totalSales') or 0.0),
            transactions=summary_row_dict.get('transactions') or 0,
            refunds=total_refund_amount,
            cashInDrawer=cash_drawer.actual,
            discrepancy=cash_drawer.discrepancy
        )
        payment_summary = PaymentSummary(
            cashAmount=float(summary_row_dict.get('cashAmount') or 0.0),
            gcashAmount=float(summary_row_dict.get('gcashAmount') or 0.0)
        )
        
        return AlignedSalesReportResponse(
            summary=summary, 
            paymentSummary=payment_summary, 
            categoryBreakdown=category_breakdown,
            productBreakdown=product_breakdown, 
            cashDrawer=cash_drawer,
            paymentMethods=payment_methods, 
            refundsList=refunds_list
        )
    except Exception as e:
        logger.error(f"Error generating aligned sales report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate sales report.")
    finally:
        if conn: 
            await conn.close()


@router_sales_metrics.post(
    "/monitoring",
    response_model=SalesMonitoringResponse,
    summary="Get Sales Monitoring Data with Filters"
)
async def get_sales_monitoring_data(
    request: SalesMonitoringRequest,
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["admin", "manager", "cashier"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied.")
    
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            date_condition = ""
            params = []
            
            # --- FIXED/IMPROVED DATE LOGIC ---
            if request.dateRange == 'today':
                date_condition = "AND CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)"
            elif request.dateRange == 'week':
                # SQL Server: DATEPART(weekday, GETDATE()) returns 1 (Sunday) to 7 (Saturday)
                # This correctly calculates the start of the week (Sunday for this standard logic)
                date_condition = "AND s.CreatedAt >= DATEADD(day, -DATEPART(weekday, GETDATE()) + 1, CAST(GETDATE() AS DATE))"
            elif request.dateRange == 'month':
                date_condition = "AND s.CreatedAt >= DATEADD(day, 1 - DAY(GETDATE()), CAST(GETDATE() AS DATE))"
            elif request.dateRange == 'custom' and request.startDate and request.endDate:
                # FIXED: Handle custom range (used by yesterday/last7days in frontend)
                date_condition = "AND CAST(s.CreatedAt AS DATE) BETWEEN ? AND ?"
                params.append(request.startDate)
                params.append(request.endDate)
            # ---------------------------------
            
            cashier_condition = ""
            category_condition = ""
            
            cashier_params = []
            if request.selectedCashier and request.selectedCashier != 'all':
                cashier_condition = "AND cs.CashierName = ?"
                cashier_params.append(request.selectedCashier)
            
            category_params = []
            if request.selectedCategory and request.selectedCategory != 'all':
                category_condition = "AND si.Category = ?"
                category_params.append(request.selectedCategory)
                
            # Combine params: Date range parameters must come first for the SQL query
            params = params + cashier_params + category_params
            
            # Use the SAME logic as dashboard.py - item-level discounts/promotions FIRST, then refunds
            sql = f"""
                WITH SaleItemDetails AS (
                    SELECT 
                        s.SaleID,
                        si.SaleItemID,
                        si.ItemName,
                        si.Category,
                        si.Quantity,
                        si.UnitPrice,
                        s.CreatedAt,
                        cs.CashierName,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                    JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                    WHERE s.Status = 'completed'
                    {date_condition} {cashier_condition} {category_condition}
                ),
                ItemLevelDiscounts AS (
                    SELECT 
                        sid.SaleItemID,
                        SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sid.SaleItemID
                    )
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT 
                        sip.SaleItemID,
                        SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = sip.SaleItemID
                    )
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT 
                        ri.SaleItemID,
                        SUM(ri.RefundAmount) AS TotalRefundAmount,
                        SUM(ri.RefundedQuantity) AS RefundedQty
                    FROM RefundedItems ri
                    WHERE EXISTS (
                        SELECT 1 FROM SaleItemDetails s2 
                        WHERE s2.SaleItemID = ri.SaleItemID
                    )
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.SaleID,
                        sid.SaleItemID,
                        sid.ItemName,
                        sid.Category,
                        sid.Quantity - ISNULL(ri.RefundedQty, 0) AS NetQuantity,
                        sid.CreatedAt,
                        sid.CashierName,
                        (sid.ItemSubtotal + sid.AddonsTotal 
                         - ISNULL(ild.TotalItemDiscount, 0) 
                         - ISNULL(ilp.TotalItemPromotion, 0)) 
                        - ISNULL(ri.TotalRefundAmount, 0) AS NetAmount
                    FROM SaleItemDetails sid
                    LEFT JOIN ItemLevelDiscounts ild ON sid.SaleItemID = ild.SaleItemID
                    LEFT JOIN ItemLevelPromotions ilp ON sid.SaleItemID = ilp.SaleItemID
                    LEFT JOIN RefundedItemsDetail ri ON sid.SaleItemID = ri.SaleItemID
                    WHERE (sid.ItemSubtotal + sid.AddonsTotal 
                           - ISNULL(ild.TotalItemDiscount, 0) 
                           - ISNULL(ilp.TotalItemPromotion, 0) 
                           - ISNULL(ri.TotalRefundAmount, 0)) > 0.01
                )
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY SUM(NetAmount) DESC) as id,
                    ItemName as product,
                    Category as category,
                    SUM(NetAmount) as revenue,
                    SUM(NetAmount * 0.60) as profit,
                    SUM(NetQuantity) as quantity,
                    CAST(CreatedAt AS DATE) as date,
                    CashierName as cashier
                FROM NetSaleItems
                GROUP BY ItemName, Category, CashierName, CAST(CreatedAt AS DATE)
                ORDER BY revenue DESC
            """
            
            await cursor.execute(sql, tuple(params))
            rows = await cursor.fetchall()
            
            sales_data = []
            total_revenue = 0.0
            total_profit = 0.0
            total_quantity = 0
            
            # Get transaction count
            # Use the same date/cashier/category filters for transaction count
            count_sql = f"""
                SELECT COUNT(DISTINCT s.SaleID)
                FROM Sales s
                JOIN SaleItems si ON s.SaleID = si.SaleID
                JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                WHERE s.Status = 'completed'
                {date_condition} {cashier_condition} {category_condition}
            """
            await cursor.execute(count_sql, tuple(params))
            count_row = await cursor.fetchone()
            transaction_count = count_row[0] if count_row else 0
            
            for row in rows:
                revenue = float(row.revenue) if row.revenue else 0.0
                profit = float(row.profit) if row.profit else 0.0
                quantity = int(row.quantity) if row.quantity else 0
                
                sales_data.append(ProductSalesDetail(
                    id=row.id, product=row.product, category=row.category,
                    revenue=revenue, profit=profit, quantity=quantity,
                    date=row.date.isoformat() if row.date else date.today().isoformat(),
                    cashier=row.cashier
                ))
                
                total_revenue += revenue
                total_profit += profit
                total_quantity += quantity
            
            profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0.0
            
            return SalesMonitoringResponse(
                salesData=sales_data, 
                totalRevenue=total_revenue,
                totalProfit=total_profit, 
                totalQuantity=total_quantity,
                profitMargin=round(profit_margin, 2),
                transactionCount=transaction_count
            )
    
    except Exception as e:
        logger.error(f"Error fetching sales monitoring data: {e}", exc_info=True)
        # Detailed error message to help debugging
        raise HTTPException(status_code=500, detail=f"Failed to fetch sales monitoring data: {str(e)}")
    finally:
        if conn:
            await conn.close()