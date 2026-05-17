"""
Generate sample retail data across 3 source systems:
  1. POS terminal exports  -> CSV
  2. E-commerce orders     -> JSON
  3. Product master        -> Parquet (via pandas)
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

random.seed(7)

CATEGORIES   = ["Electronics", "Apparel", "Grocery", "Home & Kitchen", "Sports"]
REGIONS      = ["South", "North", "East", "West", "Central"]
STORE_IDS    = [f"STR-{str(i).zfill(3)}" for i in range(1, 11)]
CUSTOMER_IDS = [f"CUST-{str(i).zfill(5)}" for i in range(1, 201)]
PRODUCT_IDS  = [f"PRD-{str(i).zfill(4)}" for i in range(1, 51)]

base_date = datetime(2024, 1, 1)

# ------------------------------------------------------------------
# 1. POS CSV
# ------------------------------------------------------------------
pos_rows = []
for _ in range(500):
    d = base_date + timedelta(days=random.randint(0, 89))
    pos_rows.append({
        "order_id":       f"POS-{uuid.uuid4().hex[:8].upper()}",
        "txn_date":       d.strftime("%d/%m/%Y"),
        "store_id":       random.choice(STORE_IDS),
        "customer_id":    random.choice(CUSTOMER_IDS),
        "product_id":     random.choice(PRODUCT_IDS),
        "quantity":       random.randint(1, 10),
        "unit_price":     round(random.uniform(50, 5000), 2),
        "discount_pct":   round(random.uniform(0, 0.3), 2),
        "payment_mode":   random.choice(["Cash", "Card", "UPI"]),
        "source_system":  "POS"
    })

with open("pos_sales.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=pos_rows[0].keys())
    writer.writeheader()
    writer.writerows(pos_rows)

print(f"POS CSV: {len(pos_rows)} rows")

# ------------------------------------------------------------------
# 2. E-Commerce JSON
# ------------------------------------------------------------------
ecom_orders = []
for _ in range(300):
    d = base_date + timedelta(days=random.randint(0, 89))
    ecom_orders.append({
        "OrderID":         f"ECO-{uuid.uuid4().hex[:8].upper()}",
        "OrderDate":       d.strftime("%Y-%m-%dT%H:%M:%S"),
        "CustomerRef":     random.choice(CUSTOMER_IDS),
        "ProductSKU":      random.choice(PRODUCT_IDS),
        "Qty":             random.randint(1, 5),
        "ListPrice":       round(random.uniform(100, 8000), 2),
        "CouponDiscount":  round(random.uniform(0, 0.25), 2),
        "DeliveryRegion":  random.choice(REGIONS),
        "Channel":         "Online",
        "SourceSystem":    "ECOMMERCE"
    })

with open("ecommerce_orders.json", "w") as f:
    for o in ecom_orders:
        f.write(json.dumps(o) + "\n")

print(f"E-Commerce JSON: {len(ecom_orders)} orders")

# ------------------------------------------------------------------
# 3. Product Master Parquet
# ------------------------------------------------------------------
products = []
for pid in PRODUCT_IDS:
    products.append({
        "product_id":   pid,
        "product_name": f"Product {pid}",
        "category":     random.choice(CATEGORIES),
        "brand":        random.choice(["BrandA", "BrandB", "BrandC", "BrandD"]),
        "cost_price":   round(random.uniform(30, 3000), 2),
        "list_price":   round(random.uniform(50, 5000), 2),
        "is_active":    True,
        "effective_date": "2024-01-01",
        "end_date":     None,
        "is_current":   True
    })

pd.DataFrame(products).to_parquet("product_master.parquet", index=False)
print(f"Product Master Parquet: {len(products)} records")
