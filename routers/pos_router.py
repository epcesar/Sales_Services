from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, Field
from typing import List, Optional
from decimal import Decimal
import json
import sys
import os
import asyncio
import httpx
import logging

# --- Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_connection

# --- Auth and Service URL Configuration ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://authservices-npr8.onrender.com/auth/token")
USER_SERVICE_ME_URL = "https://authservices-npr8.onrender.com/auth/users/me"

# --- Blockchain Service URL ---
BLOCKCHAIN_LOG_URL = "https://blockchainservices.onrender.com/blockchain/log"

# --- URLs for Inventory Deduction Endpoints ---
INGREDIENTS_DEDUCT_URL = "https://bleu-stockservices.onrender.com/ingredients/deduct-from-sale"
MATERIALS_DEDUCT_URL = "https://bleu-stockservices.onrender.com/materials/deduct-from-sale"
MERCHANDISE_DEDUCT_URL = "https://bleu-stockservices.onrender.com/merchandise/deduct-from-sale"

router_sales = APIRouter(prefix="/auth/sales", tags=["sales"])

# --- Pydantic Models ---
class AddonDetail(BaseModel):
    addonId: int
    addonName: str
    price: float
    quantity: int

class SaleItem(BaseModel):
    id: int 
    name: str
    quantity: int
    price: float
    category: str
    addons: List[AddonDetail] = []
    type: Optional[str] = "product"

class ItemDiscountDetail(BaseModel):
    itemIndex: int
    quantity: int
    discountAmount: float

class AppliedDiscountDetail(BaseModel):
    discountName: str
    discountId: int
    itemDiscounts: List[ItemDiscountDetail] = []

# NEW: Promotion models
class ItemPromotionDetail(BaseModel):
    itemIndex: int
    quantity: int
    promotionAmount: float

class AppliedPromotionDetail(BaseModel):
    promotionName: str
    promotionId: int
    itemPromotions: List[ItemPromotionDetail] = []

class Sale(BaseModel):
    cartItems: List[SaleItem]
    orderType: str
    paymentMethod: str
    appliedDiscounts: List[AppliedDiscountDetail] = []
    appliedPromotions: List[AppliedPromotionDetail] = []  # NEW
    promotionalDiscountAmount: Optional[float] = 0.0
    promotionalDiscountName: Optional[str] = None
    manualDiscountAmount: Optional[float] = 0.0
    gcashReference: Optional[str] = None


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
            raise HTTPException(status_code=e.response.status_code, detail=f"Invalid token or user not found: {e.response.text}")
        except httpx.RequestError:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Could not connect to the authentication service.")
        
async def get_active_session(cashier_username: str, cursor):
    """Get the active session for a cashier"""
    await cursor.execute(
        "SELECT SessionID FROM CashierSessions WHERE CashierName = ? AND Status = 'Active'",
        cashier_username
    )
    session = await cursor.fetchone()
    if not session:
        raise HTTPException(
            status_code=400,
            detail=f"No active session found for cashier '{cashier_username}'. Please start a session first."
        )
    return session.SessionID


# --- Blockchain Logging Helper ---
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
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                payload = {
                    "service_identifier": service_identifier,
                    "action": action,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "actor_username": actor_username,
                    "change_description": change_description,
                    "data": data
                }
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                response = await client.post(BLOCKCHAIN_LOG_URL, json=payload, headers=headers)
                response.raise_for_status()
                result = response.json()
                logger.info(f"✅ Blockchain log created: TX {result.get('transaction_hash')} for {entity_type} ID {entity_id}")
                return  # Success, exit function
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Blockchain logging attempt {attempt + 1} failed for {entity_type} {entity_id}: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            else:
                logger.error(f"❌ Blockchain logging failed for {entity_type} {entity_id} after {max_retries} attempts: {e}", exc_info=True)


# --- Helper to call Inventory Services ---
async def trigger_inventory_deduction(url: str, cart_items: List[SaleItem], token: str, inventory_type: str):
    logger.info(f"Triggering {inventory_type.upper()} deduction.")
    payload = {
        "cartItems": [
            {
                "name": item.name,
                "quantity": item.quantity,
                "addons": [addon.dict() for addon in item.addons]
            }
            for item in cart_items
        ]
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                logger.info(f"Successfully requested {inventory_type.upper()} deduction.")
                return  # Success, exit function
        except Exception as e:
            error_text = str(e)
            if hasattr(e, 'response') and e.response:
                try:
                    error_text = e.response.json().get('detail', e.response.text)
                except:
                    error_text = e.response.text
            if attempt < max_retries - 1:
                logger.warning(f"{inventory_type.upper()} deduction attempt {attempt + 1} failed: {error_text}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            else:
                logger.critical(f"{inventory_type.upper()}-SYNC-FAILURE: Sale processed, but failed to deduct after {max_retries} attempts. Error: {error_text}")


# --- Background task to handle inventory deductions ---
async def process_inventory_deductions_background(products: List[SaleItem], merchandise: List[SaleItem], token: str):
    try:
        if products:
            await trigger_inventory_deduction(INGREDIENTS_DEDUCT_URL, cart_items=products, token=token, inventory_type="Ingredient")
            await trigger_inventory_deduction(MATERIALS_DEDUCT_URL, cart_items=products, token=token, inventory_type="Material")
        if merchandise:
            await trigger_inventory_deduction(MERCHANDISE_DEDUCT_URL, cart_items=merchandise, token=token, inventory_type="Merchandise")
    except Exception as e:
        logger.error(f"Background inventory deduction error: {e}", exc_info=True)


# --- Helper function to separate items by type ---
def separate_cart_items_by_type(cart_items: List[SaleItem]):
    products = []
    merchandise = []
    for item in cart_items:
        if hasattr(item, 'type') and item.type == 'merchandise' or item.category == 'Merchandise':
            merchandise.append(item)
        else:
            products.append(item)
    return products, merchandise


# --- Helper function to process applied discounts ---
async def process_applied_discounts(sale_data: Sale, cursor):
    total_manual_discount = Decimal(str(sale_data.manualDiscountAmount or 0.0))
    discount_details = []
    discount_names_with_amounts = []
    
    for discount_data in sale_data.appliedDiscounts:
        await cursor.execute(
            "SELECT id, name, discount_type, discount_value FROM discounts WHERE id = ? AND status = 'active'",
            discount_data.discountId
        )
        db_discount = await cursor.fetchone()
        
        if not db_discount:
            logger.warning(f"Discount ID {discount_data.discountId} not found or inactive")
            continue
        
        item_discounts_list = []
        for item_disc in discount_data.itemDiscounts:
            item_discounts_list.append({
                'item_index': item_disc.itemIndex,
                'quantity': item_disc.quantity,
                'discount_amount': item_disc.discountAmount
            })
        
        discount_info = {
            'id': db_discount.id,
            'name': db_discount.name,
            'item_discounts': item_discounts_list
        }
        discount_details.append(discount_info)
        
        total_disc_amount = sum(item['discount_amount'] for item in item_discounts_list)
        discount_names_with_amounts.append({
            "name": db_discount.name,
            "type": db_discount.discount_type,
            "amount": float(total_disc_amount)
        })
    
    return discount_details, discount_names_with_amounts


# NEW: Helper function to process applied promotions
async def process_applied_promotions(sale_data: Sale, cursor):
    promotion_details = []
    promotion_names_with_amounts = []
    
    for promo_data in sale_data.appliedPromotions:
        await cursor.execute(
            "SELECT id, name, promotion_type, promotion_value FROM promotions WHERE id = ? AND status = 'active'",
            promo_data.promotionId
        )
        db_promo = await cursor.fetchone()
        
        if not db_promo:
            logger.warning(f"Promotion ID {promo_data.promotionId} not found or inactive")
            continue
        
        item_promotions_list = []
        for item_promo in promo_data.itemPromotions:
            item_promotions_list.append({
                'item_index': item_promo.itemIndex,
                'quantity': item_promo.quantity,
                'promotion_amount': item_promo.promotionAmount
            })
        
        promo_info = {
            'id': db_promo.id,
            'name': db_promo.name,
            'item_promotions': item_promotions_list
        }
        promotion_details.append(promo_info)
        
        total_promo_amount = sum(item['promotion_amount'] for item in item_promotions_list)
        promotion_names_with_amounts.append({
            "name": db_promo.name,
            "type": db_promo.promotion_type,
            "amount": float(total_promo_amount)
        })
    
    return promotion_details, promotion_names_with_amounts


# --- Save item-level discounts ---
async def apply_item_level_discounts(sale_id: int, applied_discounts_details: list, items_data: list, cursor):
    for discount_data in applied_discounts_details:
        discount_id = discount_data['id']
        
        if 'item_discounts' not in discount_data:
            continue
            
        for item_discount in discount_data['item_discounts']:
            item_index = item_discount['item_index']
            quantity_discounted = item_discount['quantity']
            discount_amount = item_discount['discount_amount']
            
            if item_index >= len(items_data):
                logger.error(f"Invalid item_index {item_index} for sale {sale_id}")
                continue
                
            sale_item_id = items_data[item_index]['sale_item_id']
            
            if quantity_discounted > 0 and discount_amount > 0:
                sql_item_discount = """
                    INSERT INTO SaleItemDiscounts 
                    (SaleItemID, DiscountID, QuantityDiscounted, DiscountAmount)
                    VALUES (?, ?, ?, ?)
                """
                await cursor.execute(
                    sql_item_discount,
                    sale_item_id,
                    discount_id,
                    quantity_discounted,
                    Decimal(str(discount_amount))
                )
                logger.info(
                    f"✅ Applied discount (ID: {discount_id}) to {quantity_discounted} "
                    f"units of SaleItem {sale_item_id}, amount: ₱{discount_amount:.2f}"
                )


# NEW: Save item-level promotions
async def apply_item_level_promotions(sale_id: int, applied_promotions_details: list, items_data: list, cursor):
    """Save item-level promotion tracking"""
    for promo_data in applied_promotions_details:
        promotion_id = promo_data['id']
        
        if 'item_promotions' not in promo_data:
            continue
            
        for item_promo in promo_data['item_promotions']:
            item_index = item_promo['item_index']
            quantity_promoted = item_promo['quantity']
            promo_amount = item_promo['promotion_amount']
            
            if item_index >= len(items_data):
                logger.error(f"Invalid item_index {item_index} for sale {sale_id}")
                continue
                
            sale_item_id = items_data[item_index]['sale_item_id']
            
            if quantity_promoted > 0 and promo_amount > 0:
                sql_item_promo = """
                    INSERT INTO SaleItemPromotions 
                    (SaleItemID, PromotionID, QuantityPromoted, PromotionAmount)
                    VALUES (?, ?, ?, ?)
                """
                await cursor.execute(
                    sql_item_promo,
                    sale_item_id,
                    promotion_id,
                    quantity_promoted,
                    Decimal(str(promo_amount))
                )
                logger.info(
                    f"✅ Applied promotion (ID: {promotion_id}) to {quantity_promoted} "
                    f"units of SaleItem {sale_item_id}, amount: ₱{promo_amount:.2f}"
                )


# --- Helper function to build detailed change description ---
def build_detailed_change_description(
    items_data: list,
    final_total: float,
    payment_method: str,
    discount_names: list,
    promo_names: list,  # NEW
    promo_discount: float
) -> str:
    items_summary = []
    for item in items_data:
        item_desc = f"{item['name']} (₱{item['price']:.2f})"
        if item.get('addons') and len(item['addons']) > 0:
            addon_parts = []
            for addon in item['addons']:
                addon_parts.append(f"{addon['addon_name']} (₱{addon['price']:.2f})")
            item_desc += f" with {', '.join(addon_parts)}"
        items_summary.append(item_desc)
    
    item_count = len(items_data)
    item_word = "item" if item_count == 1 else "items"
    description = f"New sale created with {item_count} {item_word}: {items_summary[0]}"
    
    if item_count > 1:
        for additional_item in items_summary[1:]:
            description += f", {additional_item}"
    
    if discount_names:
        discount_parts = []
        for disc in discount_names:
            discount_parts.append(f"{disc['name']} (-₱{disc['amount']:.2f})")
        description += f" | Discounts: {', '.join(discount_parts)}"
    
    # NEW: Add promotion info
    if promo_names:
        promo_parts = []
        for promo in promo_names:
            promo_parts.append(f"{promo['name']} (-₱{promo['amount']:.2f})")
        description += f" | Promotions: {', '.join(promo_parts)}"
    elif promo_discount > 0:
        description += f" | Promotional Discount (-₱{promo_discount:.2f})"
    
    description += f" | Total: ₱{final_total:.2f} | Payment: {payment_method}"
    return description


@router_sales.post("/", status_code=status.HTTP_201_CREATED)
async def create_sale(
    sale: Sale, 
    background_tasks: BackgroundTasks,
    token: str = Depends(oauth2_scheme),
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["cashier"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You do not have permission to create a sale.")

    conn = None
    try:
        conn = await get_db_connection()
        conn.autocommit = False 
        
        async with conn.cursor() as cursor:
            # Calculate subtotal
            subtotal = Decimal('0.0')
            for item in sale.cartItems:
                item_price = Decimal(str(item.price))
                addons_price = Decimal('0.0')
                if item.addons:
                    for addon in item.addons:
                        addons_price += Decimal(str(addon.price)) * addon.quantity
                subtotal += (item_price + addons_price) * item.quantity
            
            # Process applied discounts
            discount_details, discount_names = await process_applied_discounts(sale, cursor)
            
            # NEW: Process applied promotions
            promotion_details, promotion_names = await process_applied_promotions(sale, cursor)
            
            manual_discount = Decimal(str(sale.manualDiscountAmount or 0.0))
            promo_discount = Decimal(str(sale.promotionalDiscountAmount or 0.0))
            cashier_name = current_user.get("username", "SystemUser")
            
            # Get active session
            session_id = await get_active_session(cashier_name, cursor)

            # Insert sale record
            sql_sale = """
                DECLARE @InsertedSales TABLE (SaleID INT);
                
                INSERT INTO Sales (
                    OrderType, PaymentMethod, SessionID,
                    TotalDiscountAmount, PromotionalDiscountAmount, 
                    GCashReferenceNumber, Status
                ) 
                OUTPUT INSERTED.SaleID INTO @InsertedSales
                VALUES (?, ?, ?, ?, ?, ?, 'processing');
                
                SELECT SaleID FROM @InsertedSales;
            """
            
            await cursor.execute(
                sql_sale, 
                sale.orderType, sale.paymentMethod, session_id, 
                manual_discount, promo_discount, 
                sale.gcashReference
            )
            
            while not cursor.description:
                if not await cursor.nextset():
                    break

            sale_id_row = await cursor.fetchone()
            if not sale_id_row or not sale_id_row[0]:
                raise HTTPException(status_code=500, detail="Failed to create sale record, starting rollback.")
            sale_id = sale_id_row[0]

            # Insert sale items
            items_data = []
            for item in sale.cartItems:
                sql_item = """
                    DECLARE @InsertedItems TABLE (SaleItemID INT);
                    
                    INSERT INTO SaleItems (SaleID, ItemName, Quantity, UnitPrice, Category) 
                    OUTPUT INSERTED.SaleItemID INTO @InsertedItems
                    VALUES (?, ?, ?, ?, ?);
                    
                    SELECT SaleItemID FROM @InsertedItems;
                """
                await cursor.execute(sql_item, sale_id, item.name, item.quantity, Decimal(str(item.price)), item.category)
                
                while not cursor.description:
                    if not await cursor.nextset():
                        break
                
                sale_item_id_row = await cursor.fetchone()
                if not sale_item_id_row or not sale_item_id_row[0]:
                    raise HTTPException(status_code=500, detail=f"Failed to insert sale item: {item.name}")
                sale_item_id = sale_item_id_row[0]
                
                item_data = {
                    "sale_item_id": sale_item_id,
                    "name": item.name,
                    "quantity": item.quantity,
                    "price": float(item.price),
                    "category": item.category,
                    "addons": []
                }
                
                # Insert addons
                if item.addons:
                    for addon in item.addons:
                        await cursor.execute("SELECT 1 FROM Addons WHERE AddonID = ?", addon.addonId)
                        if not await cursor.fetchone():
                            await cursor.execute("SET IDENTITY_INSERT dbo.Addons ON;")
                            await cursor.execute("INSERT INTO Addons (AddonID, AddonName, Price) VALUES (?, ?, ?)", addon.addonId, addon.addonName, Decimal(str(addon.price)))
                            await cursor.execute("SET IDENTITY_INSERT dbo.Addons OFF;")
                        
                        await cursor.execute("INSERT INTO SaleItemAddons (SaleItemID, AddonID, Quantity) VALUES (?, ?, ?)", sale_item_id, addon.addonId, addon.quantity)
                        
                        item_data["addons"].append({
                            "addon_id": addon.addonId,
                            "addon_name": addon.addonName,
                            "price": float(addon.price),
                            "quantity": addon.quantity
                        })
                
                items_data.append(item_data)

            # Aggregate discounts by ID to avoid PRIMARY KEY violation
            aggregated_discounts = {}
            for discount in discount_details:
                discount_id = discount['id']
                total_discount_for_this = sum(
                    item['discount_amount'] for item in discount.get('item_discounts', [])
                )
                
                if discount_id in aggregated_discounts:
                    aggregated_discounts[discount_id]['amount'] += total_discount_for_this
                    aggregated_discounts[discount_id]['item_discounts'].extend(
                        discount.get('item_discounts', [])
                    )
                else:
                    aggregated_discounts[discount_id] = {
                        'id': discount_id,
                        'name': discount['name'],
                        'amount': total_discount_for_this,
                        'item_discounts': discount.get('item_discounts', []).copy()
                    }

            # Insert aggregated sale-level discounts
            for discount_id, discount_data in aggregated_discounts.items():
                sql_sale_discount = """
                    INSERT INTO SaleDiscounts (SaleID, DiscountID, DiscountAppliedAmount) 
                    VALUES (?, ?, ?)
                """
                await cursor.execute(
                    sql_sale_discount, 
                    sale_id, 
                    discount_id, 
                    Decimal(str(discount_data['amount']))
                )
                logger.info(
                    f"✅ Inserted SaleDiscount: SaleID={sale_id}, "
                    f"DiscountID={discount_id}, Amount=₱{discount_data['amount']:.2f}"
                )

            # Insert item-level discount tracking
            aggregated_discount_details = list(aggregated_discounts.values())
            await apply_item_level_discounts(sale_id, aggregated_discount_details, items_data, cursor)

            # NEW: Aggregate and insert promotions
            aggregated_promotions = {}
            for promo in promotion_details:
                promo_id = promo['id']
                total_promo_for_this = sum(
                    item['promotion_amount'] for item in promo.get('item_promotions', [])
                )
                
                if promo_id in aggregated_promotions:
                    aggregated_promotions[promo_id]['amount'] += total_promo_for_this
                    aggregated_promotions[promo_id]['item_promotions'].extend(
                        promo.get('item_promotions', [])
                    )
                else:
                    aggregated_promotions[promo_id] = {
                        'id': promo_id,
                        'name': promo['name'],
                        'amount': total_promo_for_this,
                        'item_promotions': promo.get('item_promotions', []).copy()
                    }

            # Insert aggregated sale-level promotions
            for promo_id, promo_data in aggregated_promotions.items():
                sql_sale_promo = """
                    INSERT INTO SalePromotions (SaleID, PromotionID, DiscountApplied) 
                    VALUES (?, ?, ?)
                """
                await cursor.execute(
                    sql_sale_promo, 
                    sale_id, 
                    promo_id, 
                    Decimal(str(promo_data['amount']))
                )
                logger.info(
                    f"✅ Inserted SalePromotion: SaleID={sale_id}, "
                    f"PromotionID={promo_id}, Amount=₱{promo_data['amount']:.2f}"
                )

            # NEW: Insert item-level promotion tracking
            aggregated_promotion_details = list(aggregated_promotions.values())
            await apply_item_level_promotions(sale_id, aggregated_promotion_details, items_data, cursor)

            await conn.commit()
            
        # Calculate final totals
        total_combined_discount = manual_discount + promo_discount
        final_total = subtotal - total_combined_discount
        
        # Build description for blockchain
        detailed_description = build_detailed_change_description(
            items_data=items_data,
            final_total=float(final_total),
            payment_method=sale.paymentMethod,
            discount_names=discount_names,
            promo_names=promotion_names,  # NEW
            promo_discount=float(promo_discount)
        )
        
        blockchain_data = {
            "sale_id": sale_id,
            "order_type": sale.orderType,
            "payment_method": sale.paymentMethod,
            "cashier_name": cashier_name,
            "session_id": session_id,
            "items": items_data,
            "subtotal": float(subtotal),
            "manual_discount": float(manual_discount),
            "promotional_discount": float(promo_discount),
            "total_discount": float(total_combined_discount),
            "final_total": float(final_total),
            "gcash_reference": sale.gcashReference,
            "status": "processing",
            "applied_discounts": discount_names,
            "applied_promotions": promotion_names,  # NEW
            "promotional_discount_applied": float(promo_discount) > 0,
            "total_items": len(sale.cartItems),
            "total_quantity": sum(item.quantity for item in sale.cartItems)
        }
        
        # Background tasks
        products, merchandise = separate_cart_items_by_type(sale.cartItems)
        background_tasks.add_task(process_inventory_deductions_background, products, merchandise, current_user['access_token'])
        
        
        # NEW: Fetch employee full name before blockchain logging
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"https://bleu-stockservices.onrender.com/users/employee_name",
                    params={"username": cashier_name},
                    headers={"Authorization": f"Bearer {current_user['access_token']}"},
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    actor_full_name = data.get("employee_name") or cashier_name  # Changed to "employee_name"
                else:
                    actor_full_name = cashier_name
        except Exception as e:
            logger.error(f"Error fetching employee name: {e}")
            actor_full_name = cashier_name

        background_tasks.add_task(
            log_to_blockchain,
            service_identifier="POS_SALES",
            action="CREATE",
            entity_type="Sale",
            entity_id=sale_id,
            actor_username=actor_full_name,
            change_description=detailed_description,
            data=blockchain_data,
            token=current_user['access_token']
        )

        
        return {
            "saleId": sale_id,
            "subtotal": float(subtotal),
            "discountAmount": float(total_combined_discount),
            "finalTotal": float(final_total)
        }

    except Exception as e:
        if conn: 
            await conn.rollback()
        logger.error(f"Error processing sale: {e}", exc_info=True)
        raise e if isinstance(e, HTTPException) else HTTPException(status_code=500, detail="An unexpected error occurred.")

    finally:
        if conn:
            conn.autocommit = True 
            await conn.close()


@router_sales.get("/status/{status}")
async def get_orders_by_status(
    status: str,
    token: str = Depends(oauth2_scheme),
    current_user: dict = Depends(get_current_active_user)
):
    if current_user.get("userRole") not in ["admin", "manager", "cashier"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied.")
    
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            sql_sales = """
                SELECT s.SaleID, s.OrderType, s.PaymentMethod, s.CreatedAt, 
                       cs.CashierName, s.TotalDiscountAmount, s.PromotionalDiscountAmount, 
                       s.Status, s.GCashReferenceNumber
                FROM Sales s 
                LEFT JOIN CashierSessions cs ON s.SessionID = cs.SessionID
                WHERE s.Status = ?
                ORDER BY s.CreatedAt DESC
            """
            await cursor.execute(sql_sales, status)
            sales = await cursor.fetchall()
            
            orders = []
            for sale in sales:
                sale_id = sale.SaleID
                sql_items = """
                    SELECT si.ItemName, si.Quantity, si.UnitPrice, si.Category, si.SaleItemID 
                    FROM SaleItems si 
                    WHERE si.SaleID = ?
                """
                await cursor.execute(sql_items, sale_id)
                items = await cursor.fetchall()
                
                item_subtotal = Decimal('0.0')
                order_items = []
                total_addons_cost = Decimal('0.0')
                
                for item in items:
                    item_total = Decimal(str(item.UnitPrice)) * item.Quantity
                    item_subtotal += item_total
                    
                    # Fetch addons
                    sql_addons = """
                        SELECT a.AddonName, a.Price, sia.Quantity 
                        FROM SaleItemAddons sia 
                        JOIN Addons a ON sia.AddonID = a.AddonID 
                        WHERE sia.SaleItemID = ?
                    """
                    await cursor.execute(sql_addons, item.SaleItemID)
                    item_addons = await cursor.fetchall()
                    
                    addons_list = []
                    for addon in item_addons:
                        addon_cost = Decimal(str(addon.Price)) * addon.Quantity
                        total_addons_cost += addon_cost
                        addons_list.append({
                            'name': addon.AddonName, 
                            'price': float(addon.Price), 
                            'quantity': addon.Quantity
                        })
                    
                    # Fetch item-level discounts
                    sql_item_discounts = """
                        SELECT 
                            d.name AS DiscountName,
                            sid.QuantityDiscounted,
                            sid.DiscountAmount
                        FROM SaleItemDiscounts sid
                        JOIN discounts d ON sid.DiscountID = d.id
                        WHERE sid.SaleItemID = ?
                    """
                    await cursor.execute(sql_item_discounts, item.SaleItemID)
                    item_discount_rows = await cursor.fetchall()
                    
                    item_discounts_list = []
                    for disc_row in item_discount_rows:
                        item_discounts_list.append({
                            'discountName': disc_row.DiscountName,
                            'quantityDiscounted': disc_row.QuantityDiscounted,
                            'discountAmount': float(disc_row.DiscountAmount)
                        })
                    
                    # NEW: Fetch item-level promotions
                    sql_item_promotions = """
                        SELECT 
                            p.name AS PromotionName,
                            sip.QuantityPromoted,
                            sip.PromotionAmount
                        FROM SaleItemPromotions sip
                        JOIN promotions p ON sip.PromotionID = p.id
                        WHERE sip.SaleItemID = ?
                    """
                    await cursor.execute(sql_item_promotions, item.SaleItemID)
                    item_promotion_rows = await cursor.fetchall()
                    
                    item_promotions_list = []
                    for promo_row in item_promotion_rows:
                        item_promotions_list.append({
                            'promotionName': promo_row.PromotionName,
                            'quantityPromoted': promo_row.QuantityPromoted,
                            'promotionAmount': float(promo_row.PromotionAmount)
                        })
                    
                    order_items.append({
                        'saleItemId': item.SaleItemID,
                        'name': item.ItemName, 
                        'quantity': item.Quantity, 
                        'price': float(item.UnitPrice), 
                        'category': item.Category, 
                        'addons': addons_list,
                        'itemDiscounts': item_discounts_list,
                        'itemPromotions': item_promotions_list  # NEW
                    })
                
                manual_discount = Decimal(str(sale.TotalDiscountAmount or 0))
                promo_discount = Decimal(str(sale.PromotionalDiscountAmount or 0))
                total_combined_discount = manual_discount + promo_discount

                full_subtotal = item_subtotal + total_addons_cost
                final_total = full_subtotal - total_combined_discount
                
                orders.append({
                    'id': sale_id,
                    'orderType': sale.OrderType,
                    'paymentMethod': sale.PaymentMethod,
                    'date': sale.CreatedAt.isoformat(),
                    'status': sale.Status,
                    'cashierName': sale.CashierName or 'Unknown',
                    'gcashReference': sale.GCashReferenceNumber,
                    'orderItems': order_items,
                    'subtotal': float(full_subtotal),
                    'addOns': float(total_addons_cost),
                    'promotionalDiscount': float(promo_discount),
                    'manualDiscount': float(manual_discount),
                    'total': float(final_total)
                })
            
            return orders
            
    except Exception as e:
        logger.error(f"Error fetching orders by status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch orders")
    finally:
        if conn:
            await conn.close()