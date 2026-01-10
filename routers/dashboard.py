from fastapi import APIRouter, HTTPException, Query, Header, status, Depends
from fastapi.security import OAuth2PasswordBearer
from typing import Optional, Literal
from datetime import datetime, timedelta
from decimal import Decimal
import pyodbc
import httpx
import logging
from pydantic import BaseModel
from database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Auth Configuration
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://authservices-npr8.onrender.com/auth/token")
USER_SERVICE_ME_URL = "https://authservices-npr8.onrender.com/auth/users/me"

router_dashboard = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"]
)

# Authorization Helper Function
async def get_current_active_user(token: str = Depends(oauth2_scheme)):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                USER_SERVICE_ME_URL, 
                headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            user_data = response.json()
            user_data['access_token'] = token
            return user_data
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Invalid token or user not found: {e.response.text}",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not connect to the authentication service."
            )

# Helper function to get full name from username
async def get_full_name(username: str, auth_token: str) -> str:
    """Fetch full name from employee API, return username if fails"""
    if not username or username == 'Unknown' or not auth_token:
        return username or 'Unknown'
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://authservices-npr8.onrender.com/users/employee_name",
                params={"username": username},
                headers={"Authorization": f"Bearer {auth_token}"}
            )
            if response.status_code == 200:
                employee_data = response.json()
                # Try both 'employee_name' and 'full_name' fields
                return employee_data.get('employee_name') or employee_data.get('full_name') or username
    except Exception as e:
        logger.error(f"Error fetching employee name for {username}: {e}")
    
    return username

# Response Models
class SummaryCard(BaseModel):
    title: str
    current: float
    previous: float
    format: str
    subtext: Optional[str] = None

class RevenueData(BaseModel):
    name: str
    income: float
    expense: float

class BestSellingItem(BaseModel):
    name: str
    sales: int

class ShiftPerformance(BaseModel):
    cashier: str
    sales: float
    orders: int

class ActiveOrder(BaseModel):
    time: str
    pending: int
    inProgress: int

class CompletedOrder(BaseModel):
    hour: str
    orders: int

class CanceledOrder(BaseModel):
    name: str
    canceled: int

class SpillageItem(BaseModel):
    name: str
    cost: float
    incidents: int

class SpillageData(BaseModel):
    cost: float
    incidents: int
    target: float
    items: list[SpillageItem]


@router_dashboard.get("/admin/total-sales")
async def get_total_sales(
    filter: Literal["Today", "Yesterday", "This Week", "This Month"] = "Today",
    current_user: dict = Depends(get_current_active_user)
):
    """Get total sales data for admin dashboard - Net sales after item-level discounts/promotions and refunds"""
    # Check authorization
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Define date ranges
        now = datetime.now()
        
        if filter == "Today":
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_start = start_date - timedelta(days=1)
            prev_end = start_date
        elif filter == "Yesterday":
            start_date = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            prev_start = start_date - timedelta(days=1)
            prev_end = start_date
        elif filter == "This Week":
            start_date = now - timedelta(days=now.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_start = start_date - timedelta(days=7)
            prev_end = start_date
        else:  # This Month
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            prev_start = (start_date - timedelta(days=1)).replace(day=1)
            prev_end = start_date
        
        end_date = now
        
        # Current period sales - Using the SAME logic as cashier sales.py
        await cursor.execute("""
            WITH SaleItemDetails AS (
                SELECT 
                    s.SaleID,
                    si.SaleItemID,
                    (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                    ISNULL((
                        SELECT SUM(a.Price * sia.Quantity)
                        FROM SaleItemAddons sia
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = si.SaleItemID
                    ), 0) AS AddonsTotal
                FROM Sales s
                INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                WHERE s.Status = 'completed'
                AND s.CreatedAt >= ? AND s.CreatedAt < ?
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
            SELECT ISNULL(SUM(NetAmount), 0) AS NetSales
            FROM NetSaleItems
        """, start_date, end_date)
        current = float((await cursor.fetchone())[0] or 0)
        
        # Previous period sales - Same logic
        await cursor.execute("""
            WITH SaleItemDetails AS (
                SELECT 
                    s.SaleID,
                    si.SaleItemID,
                    (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                    ISNULL((
                        SELECT SUM(a.Price * sia.Quantity)
                        FROM SaleItemAddons sia
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = si.SaleItemID
                    ), 0) AS AddonsTotal
                FROM Sales s
                INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                WHERE s.Status = 'completed'
                AND s.CreatedAt >= ? AND s.CreatedAt < ?
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
            SELECT ISNULL(SUM(NetAmount), 0) AS NetSales
            FROM NetSaleItems
        """, prev_start, prev_end)
        previous = float((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "currency"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/admin/cash-variance")
async def get_cash_variance(
    current_user: dict = Depends(get_current_active_user)
):
    """Get cash drawer variance across all shifts"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Today's variance
        await cursor.execute("""
            SELECT ISNULL(SUM(ABS(cd.DiscrepancyAmount)), 0) as TotalVariance
            FROM CashDiscrepancies cd
            INNER JOIN CashierSessions cs ON cd.SessionID = cs.SessionID
            WHERE CAST(cd.ReportedAt AS DATE) = CAST(GETDATE() AS DATE)
        """)
        current = float((await cursor.fetchone())[0] or 0)
        
        # Yesterday's variance
        await cursor.execute("""
            SELECT ISNULL(SUM(ABS(cd.DiscrepancyAmount)), 0) as TotalVariance
            FROM CashDiscrepancies cd
            INNER JOIN CashierSessions cs ON cd.SessionID = cs.SessionID
            WHERE CAST(cd.ReportedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
        """)
        previous = float((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "currency",
            "subtext": "Across all shifts"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/admin/best-selling-product")
async def get_best_selling_product(
    filter: Literal["Today", "Last 7 Days", "Last 30 Days"] = "Today",
    current_user: dict = Depends(get_current_active_user)
):
    """Get best-selling product stats - shows 0 if no sales in selected period"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Define date conditions based on filter
        if filter == "Today":
            current_condition = "CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)"
            previous_condition = "CAST(s.CreatedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)"
        elif filter == "Last 7 Days":
            current_condition = "s.CreatedAt >= DATEADD(day, -7, GETDATE())"
            previous_condition = """s.CreatedAt >= DATEADD(day, -14, GETDATE()) 
                                   AND s.CreatedAt < DATEADD(day, -7, GETDATE())"""
        else:  # Last 30 Days
            current_condition = "s.CreatedAt >= DATEADD(day, -30, GETDATE())"
            previous_condition = """s.CreatedAt >= DATEADD(day, -60, GETDATE()) 
                                   AND s.CreatedAt < DATEADD(day, -30, GETDATE())"""
        
        # Current period - Get best-selling product
        query = f"""
            SELECT TOP 1 
                si.ItemName,
                SUM(si.Quantity) as TotalSales
            FROM SaleItems si
            INNER JOIN Sales s ON si.SaleID = s.SaleID
            WHERE s.Status = 'completed'
            AND {current_condition}
            GROUP BY si.ItemName
            ORDER BY TotalSales DESC
        """
        await cursor.execute(query)
        result = await cursor.fetchone()
        current = int(result[1]) if result else 0
        product_name = result[0] if result else "N/A"
        
        # Previous period - Get best-selling product sales count
        prev_query = f"""
            SELECT TOP 1 
                si.ItemName,
                SUM(si.Quantity) as TotalSales
            FROM SaleItems si
            INNER JOIN Sales s ON si.SaleID = s.SaleID
            WHERE s.Status = 'completed'
            AND {previous_condition}
            GROUP BY si.ItemName
            ORDER BY TotalSales DESC
        """
        await cursor.execute(prev_query)
        result = await cursor.fetchone()
        previous = int(result[1]) if result else 0
        
        return {
            "current": current,
            "previous": previous,
            "format": "number",
            "subtext": product_name
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/admin/total-refunds")
async def get_total_refunds(
    current_user: dict = Depends(get_current_active_user)
):
    """Get total refunds for today"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Today's refunds
        await cursor.execute("""
            SELECT COUNT(*) as RefundCount
            FROM RefundedOrders
            WHERE CAST(RefundedAt AS DATE) = CAST(GETDATE() AS DATE)
        """)
        current = int((await cursor.fetchone())[0] or 0)
        
        # Yesterday's refunds
        await cursor.execute("""
            SELECT COUNT(*) as RefundCount
            FROM RefundedOrders
            WHERE CAST(RefundedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
        """)
        previous = int((await cursor.fetchone())[0] or 0)
        
        # Calculate percentage of sales
        await cursor.execute("""
            SELECT 
                COUNT(DISTINCT ro.RefundID) as RefundCount,
                COUNT(DISTINCT s.SaleID) as TotalSales
            FROM Sales s
            LEFT JOIN RefundedOrders ro ON s.SaleID = ro.SaleID 
                AND CAST(ro.RefundedAt AS DATE) = CAST(GETDATE() AS DATE)
            WHERE CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
        """)
        result = await cursor.fetchone()
        refund_count = result[0] or 0
        total_sales = result[1] or 1
        percentage = (refund_count / total_sales) * 100 if total_sales > 0 else 0
        
        return {
            "current": current,
            "previous": previous,
            "format": "number",
            "subtext": f"{percentage:.1f}% of total sales"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/admin/sales-overview")
async def get_sales_overview(
    filter: Literal["Daily", "Weekly", "Monthly", "Yearly"] = "Monthly",
    current_user: dict = Depends(get_current_active_user)
):
    """Get sales overview data - Net sales after item-level discounts/promotions and refunds"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        data = []
        
        if filter == "Daily":
            await cursor.execute("""
                WITH SaleItemDetails AS (
                    SELECT 
                        DATENAME(weekday, s.CreatedAt) as Period,
                        CAST(s.CreatedAt AS DATE) as SortDate,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                    WHERE s.Status = 'completed'
                    AND s.CreatedAt >= DATEADD(day, -7, GETDATE())
                ),
                ItemLevelDiscounts AS (
                    SELECT sid.SaleItemID, SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sid.SaleItemID)
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT sip.SaleItemID, SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sip.SaleItemID)
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT ri.SaleItemID, SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = ri.SaleItemID)
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.Period, sid.SortDate,
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
                SELECT Period, SortDate, ISNULL(SUM(NetAmount), 0) AS Income
                FROM NetSaleItems
                GROUP BY Period, SortDate
                ORDER BY SortDate
            """)
        elif filter == "Weekly":
            await cursor.execute("""
                WITH SaleItemDetails AS (
                    SELECT 
                        'Week ' + CAST(DATEPART(week, s.CreatedAt) - DATEPART(week, DATEADD(month, -1, GETDATE())) + 1 AS VARCHAR) as Period,
                        DATEPART(week, s.CreatedAt) as WeekNum,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                    WHERE s.Status = 'completed'
                    AND s.CreatedAt >= DATEADD(week, -4, GETDATE())
                ),
                ItemLevelDiscounts AS (
                    SELECT sid.SaleItemID, SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sid.SaleItemID)
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT sip.SaleItemID, SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sip.SaleItemID)
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT ri.SaleItemID, SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = ri.SaleItemID)
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.Period, sid.WeekNum,
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
                SELECT Period, WeekNum, ISNULL(SUM(NetAmount), 0) AS Income
                FROM NetSaleItems
                GROUP BY Period, WeekNum
                ORDER BY WeekNum
            """)
        elif filter == "Monthly":
            await cursor.execute("""
                WITH SaleItemDetails AS (
                    SELECT 
                        DATENAME(month, s.CreatedAt) as Period,
                        YEAR(s.CreatedAt) as YearNum,
                        MONTH(s.CreatedAt) as MonthNum,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                    WHERE s.Status = 'completed'
                    AND s.CreatedAt >= DATEADD(month, -7, GETDATE())
                ),
                ItemLevelDiscounts AS (
                    SELECT sid.SaleItemID, SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sid.SaleItemID)
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT sip.SaleItemID, SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sip.SaleItemID)
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT ri.SaleItemID, SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = ri.SaleItemID)
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.Period, sid.YearNum, sid.MonthNum,
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
                SELECT Period, YearNum, MonthNum, ISNULL(SUM(NetAmount), 0) AS Income
                FROM NetSaleItems
                GROUP BY Period, YearNum, MonthNum
                ORDER BY YearNum, MonthNum
            """)
        else:  # Yearly
            await cursor.execute("""
                WITH SaleItemDetails AS (
                    SELECT 
                        CAST(YEAR(s.CreatedAt) AS VARCHAR) as Period,
                        YEAR(s.CreatedAt) as YearNum,
                        si.SaleItemID,
                        (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                        ISNULL((
                            SELECT SUM(a.Price * sia.Quantity)
                            FROM SaleItemAddons sia
                            JOIN Addons a ON sia.AddonID = a.AddonID
                            WHERE sia.SaleItemID = si.SaleItemID
                        ), 0) AS AddonsTotal
                    FROM Sales s
                    INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                    WHERE s.Status = 'completed'
                    AND s.CreatedAt >= DATEADD(year, -4, GETDATE())
                ),
                ItemLevelDiscounts AS (
                    SELECT sid.SaleItemID, SUM(sid.DiscountAmount) AS TotalItemDiscount
                    FROM SaleItemDiscounts sid
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sid.SaleItemID)
                    GROUP BY sid.SaleItemID
                ),
                ItemLevelPromotions AS (
                    SELECT sip.SaleItemID, SUM(sip.PromotionAmount) AS TotalItemPromotion
                    FROM SaleItemPromotions sip
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sip.SaleItemID)
                    GROUP BY sip.SaleItemID
                ),
                RefundedItemsDetail AS (
                    SELECT ri.SaleItemID, SUM(ri.RefundAmount) AS TotalRefundAmount
                    FROM RefundedItems ri
                    WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = ri.SaleItemID)
                    GROUP BY ri.SaleItemID
                ),
                NetSaleItems AS (
                    SELECT 
                        sid.Period, sid.YearNum,
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
                SELECT Period, YearNum, ISNULL(SUM(NetAmount), 0) AS Income
                FROM NetSaleItems
                GROUP BY Period, YearNum
                ORDER BY YearNum
            """)
        
        rows = await cursor.fetchall()
        
        if not rows:
            return []
        
        for row in rows:
            data.append({
                "name": row[0],
                "income": float(row[-1])
            })
        
        return data
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/admin/best-selling-items")
async def get_best_selling_items(
    filter: Literal["Today", "Last 7 Days", "Last 30 Days", "Last 90 Days", "All-Time"] = "Last 30 Days",
    current_user: dict = Depends(get_current_active_user)
):
    """Get best-selling items, excluding any refunded quantities."""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Define date conditions based on filter
        if filter == "Today":
            date_condition = "CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)"
        elif filter == "Last 7 Days":
            date_condition = "s.CreatedAt >= DATEADD(day, -7, GETDATE())"
        elif filter == "Last 30 Days":
            date_condition = "s.CreatedAt >= DATEADD(day, -30, GETDATE())"
        elif filter == "Last 90 Days":
            date_condition = "s.CreatedAt >= DATEADD(day, -90, GETDATE())"
        else:  # All-Time
            date_condition = "1=1"  # No date filter
        
        # Get best-selling items based on net quantity (sold - refunded)
        query = f"""
            WITH ItemNetQuantities AS (
                -- Calculate the net quantity for each individual sale item,
                -- accounting for any partial or full refunds.
                SELECT
                    si.ItemName,
                    si.Quantity - ISNULL(SUM(ri.RefundedQuantity), 0) AS NetQuantity
                FROM Sales s
                JOIN SaleItems si ON s.SaleID = si.SaleID
                LEFT JOIN RefundedItems ri ON si.SaleItemID = ri.SaleItemID
                WHERE 
                    s.Status = 'completed' AND {date_condition}
                GROUP BY si.SaleItemID, si.ItemName, si.Quantity
            )
            -- Aggregate the net quantities for each product name.
            SELECT TOP 5
                ItemName,
                SUM(NetQuantity) as TotalSales
            FROM ItemNetQuantities
            GROUP BY ItemName
            HAVING SUM(NetQuantity) > 0 -- Exclude products that have been fully refunded.
            ORDER BY TotalSales DESC
        """
        await cursor.execute(query)
        
        data = []
        rows = await cursor.fetchall()
        for row in rows:
            data.append({
                "name": row[0],
                "sales": int(row[1])
            })

        return data
    finally:
        await cursor.close()
        await conn.close()


@router_dashboard.get("/admin/shift-performance")
async def get_shift_performance(
    filter: Literal["Today", "Yesterday", "This Week", "Last Week", "This Month"] = "Today",
    current_user: dict = Depends(get_current_active_user)
):
    """Get shift performance by cashier - Net sales after item-level discounts/promotions and refunds"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        if filter == "Today":
            date_filter = "CAST(s.CreatedAt AS DATE) = CAST(GETDATE() AS DATE)"
        elif filter == "Yesterday":
            date_filter = "CAST(s.CreatedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)"
        elif filter == "This Week":
            date_filter = "s.CreatedAt >= DATEADD(week, DATEDIFF(week, 0, GETDATE()), 0)"
        elif filter == "Last Week":
            date_filter = """s.CreatedAt >= DATEADD(week, DATEDIFF(week, 0, GETDATE()) - 1, 0) 
                           AND s.CreatedAt < DATEADD(week, DATEDIFF(week, 0, GETDATE()), 0)"""
        else:  # This Month
            date_filter = "s.CreatedAt >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)"
        
        query = f"""
            WITH SaleItemDetails AS (
                SELECT 
                    ISNULL(cs.CashierName, 'Unknown') as CashierUsername,
                    s.SaleID,
                    si.SaleItemID,
                    (si.UnitPrice * si.Quantity) AS ItemSubtotal,
                    ISNULL((
                        SELECT SUM(a.Price * sia.Quantity)
                        FROM SaleItemAddons sia
                        JOIN Addons a ON sia.AddonID = a.AddonID
                        WHERE sia.SaleItemID = si.SaleItemID
                    ), 0) AS AddonsTotal
                FROM Sales s
                INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                LEFT JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                WHERE s.Status = 'completed'
                AND {date_filter}
            ),
            ItemLevelDiscounts AS (
                SELECT sid.SaleItemID, SUM(sid.DiscountAmount) AS TotalItemDiscount
                FROM SaleItemDiscounts sid
                WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sid.SaleItemID)
                GROUP BY sid.SaleItemID
            ),
            ItemLevelPromotions AS (
                SELECT sip.SaleItemID, SUM(sip.PromotionAmount) AS TotalItemPromotion
                FROM SaleItemPromotions sip
                WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = sip.SaleItemID)
                GROUP BY sip.SaleItemID
            ),
            RefundedItemsDetail AS (
                SELECT ri.SaleItemID, SUM(ri.RefundAmount) AS TotalRefundAmount
                FROM RefundedItems ri
                WHERE EXISTS (SELECT 1 FROM SaleItemDetails s2 WHERE s2.SaleItemID = ri.SaleItemID)
                GROUP BY ri.SaleItemID
            ),
            NetSaleItems AS (
                SELECT 
                    sid.CashierUsername,
                    sid.SaleID,
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
                CashierUsername,
                ISNULL(SUM(NetAmount), 0) as TotalSales,
                COUNT(DISTINCT SaleID) as OrderCount
            FROM NetSaleItems
            GROUP BY CashierUsername
            HAVING COUNT(DISTINCT SaleID) > 0
            ORDER BY TotalSales DESC
        """
        
        await cursor.execute(query)
        
        data = []
        rows = await cursor.fetchall()
        
        if not rows:
            return []
        
        auth_token = current_user.get('access_token')
        
        for row in rows:
            username = row[0]
            full_name = await get_full_name(username, auth_token)
            
            data.append({
                "cashier": full_name,
                "sales": float(row[1]),
                "orders": int(row[2])
            })
        
        return data
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/active-orders")
async def get_active_orders(
    current_user: dict = Depends(get_current_active_user)
):
    """Get current active orders count"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Current active orders
        await cursor.execute("""
            SELECT COUNT(*) as ActiveCount
            FROM Sales
            WHERE Status = 'processing'
        """)
        current = int((await cursor.fetchone())[0] or 0)
        
        # Average from last hour
        await cursor.execute("""
            SELECT COUNT(*) as ActiveCount
            FROM Sales
            WHERE Status = 'processing'
            AND CreatedAt >= DATEADD(hour, -1, GETDATE())
        """)
        previous = int((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "number",
            "subtext": "Real-time count"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/completed-orders")
async def get_completed_orders(
    current_user: dict = Depends(get_current_active_user)
):
    """Get completed orders for today"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Today's completed
        await cursor.execute("""
            SELECT COUNT(*) as CompletedCount
            FROM Sales
            WHERE Status = 'completed'
            AND CAST(CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
        """)
        current = int((await cursor.fetchone())[0] or 0)
        
        # Yesterday's completed
        await cursor.execute("""
            SELECT COUNT(*) as CompletedCount
            FROM Sales
            WHERE Status = 'completed'
            AND CAST(CreatedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
        """)
        previous = int((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "number",
            "subtext": "Daily total"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/canceled-orders")
async def get_canceled_orders(
    current_user: dict = Depends(get_current_active_user)
):
    """Get canceled orders for today"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Today's canceled
        await cursor.execute("""
            SELECT COUNT(*) as CanceledCount
            FROM Sales
            WHERE Status = 'cancelled'
            AND CAST(UpdatedAt AS DATE) = CAST(GETDATE() AS DATE)
        """)
        current = int((await cursor.fetchone())[0] or 0)
        
        # Yesterday's canceled
        await cursor.execute("""
            SELECT COUNT(*) as CanceledCount
            FROM Sales
            WHERE Status = 'cancelled'
            AND CAST(UpdatedAt AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
        """)
        previous = int((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "number",
            "subtext": "Today's cancellations"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/spillage-cost")
async def get_spillage_cost(
    current_user: dict = Depends(get_current_active_user)
):
    """Get spillage cost for today"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Today's spillage cost using actual product prices
        await cursor.execute("""
            SELECT 
                COUNT(DISTINCT ps.SpillageID) as IncidentCount,
                ISNULL(SUM(ps.Quantity * si.UnitPrice), 0) as TotalCost
            FROM ProductSpillage ps
            LEFT JOIN SaleItems si ON ps.ProductName = si.ItemName
            WHERE CAST(ps.SpillageDate AS DATE) = CAST(GETDATE() AS DATE)
            AND ps.isDeleted = 0
            AND si.SaleItemID = (
                SELECT TOP 1 SaleItemID 
                FROM SaleItems 
                WHERE ItemName = ps.ProductName 
                ORDER BY SaleItemID DESC
            )
        """)
        result = await cursor.fetchone()
        current = float(result[1] or 0)
        incidents = int(result[0] or 0)
        
        # Yesterday's spillage
        await cursor.execute("""
            SELECT ISNULL(SUM(ps.Quantity * si.UnitPrice), 0) as TotalCost
            FROM ProductSpillage ps
            LEFT JOIN SaleItems si ON ps.ProductName = si.ItemName
            WHERE CAST(ps.SpillageDate AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
            AND ps.isDeleted = 0
            AND si.SaleItemID = (
                SELECT TOP 1 SaleItemID 
                FROM SaleItems 
                WHERE ItemName = ps.ProductName 
                ORDER BY SaleItemID DESC
            )
        """)
        previous = float((await cursor.fetchone())[0] or 0)
        
        return {
            "current": current,
            "previous": previous,
            "format": "currency",
            "subtext": f"{incidents} incidents today"
        }
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/spillage-overview")
async def get_spillage_overview(
    filter: Literal["By Product Type", "By Cashier/Shift", "By Incident Reason"] = "By Product Type",
    current_user: dict = Depends(get_current_active_user)
):
    """Get spillage breakdown - returns zero values if no data"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        if filter == "By Product Type":
            await cursor.execute("""
                SELECT 
                    ps.Category as Name,
                    COUNT(DISTINCT ps.SpillageID) as Incidents,
                    ISNULL(SUM(ps.Quantity * si.UnitPrice), 0) as TotalCost
                FROM ProductSpillage ps
                LEFT JOIN SaleItems si ON ps.ProductName = si.ItemName
                WHERE CAST(ps.SpillageDate AS DATE) = CAST(GETDATE() AS DATE)
                AND ps.isDeleted = 0
                AND si.SaleItemID = (
                    SELECT TOP 1 SaleItemID 
                    FROM SaleItems 
                    WHERE ItemName = ps.ProductName 
                    ORDER BY SaleItemID DESC
                )
                GROUP BY ps.Category
                HAVING COUNT(DISTINCT ps.SpillageID) > 0
                ORDER BY Incidents DESC
            """)
        elif filter == "By Cashier/Shift":
            # Join with CashierSessions to get the cashier name
            await cursor.execute("""
                SELECT 
                    cs.CashierName as Name,
                    COUNT(DISTINCT ps.SpillageID) as Incidents,
                    ISNULL(SUM(ps.Quantity * si.UnitPrice), 0) as TotalCost
                FROM ProductSpillage ps
                INNER JOIN CashierSessions cs ON ps.SessionID = cs.SessionID
                LEFT JOIN SaleItems si ON ps.ProductName = si.ItemName
                WHERE CAST(ps.SpillageDate AS DATE) = CAST(GETDATE() AS DATE)
                AND ps.isDeleted = 0
                AND si.SaleItemID = (
                    SELECT TOP 1 SaleItemID 
                    FROM SaleItems 
                    WHERE ItemName = ps.ProductName 
                    ORDER BY SaleItemID DESC
                )
                GROUP BY cs.CashierName
                HAVING COUNT(DISTINCT ps.SpillageID) > 0
                ORDER BY Incidents DESC
            """)
        else:  # By Incident Reason
            await cursor.execute("""
                SELECT 
                    ps.Reason as Name,
                    COUNT(DISTINCT ps.SpillageID) as Incidents,
                    ISNULL(SUM(ps.Quantity * si.UnitPrice), 0) as TotalCost
                FROM ProductSpillage ps
                LEFT JOIN SaleItems si ON ps.ProductName = si.ItemName
                WHERE CAST(ps.SpillageDate AS DATE) = CAST(GETDATE() AS DATE)
                AND ps.isDeleted = 0
                AND si.SaleItemID = (
                    SELECT TOP 1 SaleItemID 
                    FROM SaleItems 
                    WHERE ItemName = ps.ProductName 
                    ORDER BY SaleItemID DESC
                )
                GROUP BY ps.Reason
                HAVING COUNT(DISTINCT ps.SpillageID) > 0
                ORDER BY Incidents DESC
            """)
        
        rows = await cursor.fetchall()
        total_cost = sum(float(row[2]) for row in rows) if rows else 0
        total_incidents = sum(int(row[1]) for row in rows) if rows else 0
        
        items = []
        
        # If filter is by cashier, fetch full names
        if filter == "By Cashier/Shift" and rows:
            auth_token = current_user.get('access_token')
            
            for row in rows:
                username = row[0]
                full_name = await get_full_name(username, auth_token)
                
                items.append({
                    "name": full_name,
                    "incidents": int(row[1]),
                    "cost": float(row[2])
                })
        else:
            # For non-cashier filters, use name as-is
            for row in rows:
                items.append({
                    "name": row[0],
                    "incidents": int(row[1]),
                    "cost": float(row[2])
                })
        
        # Calculate target (assuming budget of 3000 per day)
        budget = 3000
        target = ((budget - total_cost) / budget) * 100 if budget > 0 else 0
        
        return {
            "cost": total_cost,
            "incidents": total_incidents,
            "target": round(target, 1),
            "items": items
        }
    finally:
        await cursor.close()
        await conn.close()
        
@router_dashboard.get("/manager/active-orders-monitor")
async def get_active_orders_monitor(
    filter: Literal["Real-time", "Last 4 Hours", "Full Day"] = "Real-time",
    current_user: dict = Depends(get_current_active_user)
):
    """Get active orders over time"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        # Get current time
        now = datetime.now()
        current_time = now.strftime("%I:%M %p")
        
        # Get current pending orders
        await cursor.execute("""
            SELECT COUNT(*) as PendingCount
            FROM Sales
            WHERE Status = 'pending'
        """)
        pending_count = int((await cursor.fetchone())[0] or 0)
        
        # Get current in-progress orders
        await cursor.execute("""
            SELECT COUNT(*) as ProcessingCount
            FROM Sales
            WHERE Status = 'processing'
        """)
        inprogress_count = int((await cursor.fetchone())[0] or 0)
        
        # Return only current status (not historical)
        data = [{
            "time": current_time,
            "pending": pending_count,
            "inProgress": inprogress_count
        }]
        
        return data
        
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/completed-orders-peak")
async def get_completed_orders_peak(
    filter: Literal["Today", "Average Last 7 Days", "Last Same Day"] = "Today",
    current_user: dict = Depends(get_current_active_user)
):
    """Get completed orders by hour - returns empty array if no data"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        if filter == "Today":
            await cursor.execute("""
                SELECT 
                    DATEPART(hour, CreatedAt) as Hour,
                    COUNT(*) as OrderCount
                FROM Sales
                WHERE Status = 'completed'
                AND CAST(CreatedAt AS DATE) = CAST(GETDATE() AS DATE)
                GROUP BY DATEPART(hour, CreatedAt)
                HAVING COUNT(*) > 0
                ORDER BY Hour
            """)
        elif filter == "Average Last 7 Days":
            await cursor.execute("""
                SELECT 
                    DATEPART(hour, CreatedAt) as Hour,
                    COUNT(*) / 7 as OrderCount
                FROM Sales
                WHERE Status = 'completed'
                AND CreatedAt >= DATEADD(day, -7, GETDATE())
                GROUP BY DATEPART(hour, CreatedAt)
                HAVING COUNT(*) > 0
                ORDER BY Hour
            """)
        else:  # Last Same Day
            await cursor.execute("""
                SELECT 
                    DATEPART(hour, CreatedAt) as Hour,
                    COUNT(*) as OrderCount
                FROM Sales
                WHERE Status = 'completed'
                AND CAST(CreatedAt AS DATE) = CAST(DATEADD(day, -7, GETDATE()) AS DATE)
                GROUP BY DATEPART(hour, CreatedAt)
                HAVING COUNT(*) > 0
                ORDER BY Hour
            """)
        
        rows = await cursor.fetchall()
        
        # Return empty array if no data
        if not rows:
            return []
        
        data = []
        for row in rows:
            hour = int(row[0])
            next_hour = (hour + 1) % 24
            data.append({
                "hour": f"{hour}-{next_hour}",
                "orders": int(row[1])
            })
        
        return data
    finally:
        await cursor.close()
        await conn.close()

@router_dashboard.get("/manager/canceled-orders-analysis")
async def get_canceled_orders_analysis(
    filter: Literal["By Cashier", "By Product Category", "By Cancellation Reason"] = "By Cashier",
    current_user: dict = Depends(get_current_active_user)
):
    """Get canceled orders breakdown - returns empty array if no data"""
    allowed_roles = ["admin", "manager"]
    if current_user.get("userRole") not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to view this data."
        )
    
    conn = await get_db_connection()
    cursor = await conn.cursor()
    
    try:
        if filter == "By Cashier":
            await cursor.execute("""
                SELECT 
                    ISNULL(cs.CashierName, 'Unknown') as Name,
                    COUNT(*) as CanceledCount
                FROM Sales s
                LEFT JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                WHERE s.Status = 'cancelled'
                AND CAST(s.UpdatedAt AS DATE) = CAST(GETDATE() AS DATE)
                GROUP BY cs.CashierName
                HAVING COUNT(*) > 0
                ORDER BY CanceledCount DESC
            """)
            
            rows = await cursor.fetchall()
            
            # Return empty array if no data
            if not rows:
                return []
            
            # Fetch full names for cashiers
            data = []
            auth_token = current_user.get('access_token')
            
            for row in rows:
                username = row[0]
                full_name = await get_full_name(username, auth_token)
                
                data.append({
                    "name": full_name,
                    "canceled": int(row[1])
                })
            
            return data
            
        elif filter == "By Product Category":
            await cursor.execute("""
                SELECT 
                    si.Category as Name,
                    COUNT(DISTINCT s.SaleID) as CanceledCount
                FROM Sales s
                INNER JOIN SaleItems si ON s.SaleID = si.SaleID
                WHERE s.Status = 'cancelled'
                AND CAST(s.UpdatedAt AS DATE) = CAST(GETDATE() AS DATE)
                GROUP BY si.Category
                HAVING COUNT(DISTINCT s.SaleID) > 0
                ORDER BY CanceledCount DESC
            """)
        else:  # By Cancellation Reason
            # If you don't have a reason field, return empty array
            return []
        
        data = []
        rows = await cursor.fetchall()
        
        # Return empty array if no data
        if not rows:
            return []
        
        for row in rows:
            data.append({
                "name": row[0],
                "canceled": int(row[1])
            })
        
        return data
    finally:
        await cursor.close()
        await conn.close()