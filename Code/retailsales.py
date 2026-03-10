from zoneinfo import ZoneInfo
from datetime import datetime
import pandas as pd
import hashlib
from itertools import cycle

# Product-level dataset
df1 = pd.read_csv("business.retailsales.csv")
df1.columns = df1.columns.str.strip()

# Monthly dataset only for month/year mapping
df2 = pd.read_csv("business.retailsales2.csv")
df2.columns = df2.columns.str.strip()

# Stable product_id
def make_product_id(product_type) -> str:
    if pd.isna(product_type):
        product_type = "UNKNOWN_PRODUCT"
    product_type = str(product_type).strip()
    return "P" + hashlib.md5(product_type.encode("utf-8")).hexdigest()[:8].upper()

df1["product_id"] = df1["Product Type"].apply(make_product_id)

# Build repeating month-year mapping from dataset2
month_year_pairs = df2[["Month", "Year"]].drop_duplicates().to_dict(orient="records")
month_year_cycle = cycle(month_year_pairs)

# Assign dataset-based month/year to dataset1 rows
assigned_pairs = [next(month_year_cycle) for _ in range(len(df1))]
df1["Month"] = [x["Month"] for x in assigned_pairs]
df1["Year"] = [x["Year"] for x in assigned_pairs]

# Event iterator
rows1 = cycle(df1.to_dict(orient="records"))

def send_retail_sales_data():
    row = next(rows1)

    event = {
        "source": "product_sales",
        "event_id": f"E{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        "event_timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "product_id": row["product_id"],
        "product_type": str(row["Product Type"]).strip(),
        "month": row["Month"],
        "year": int(row["Year"]),
        "net_quantity": int(row["Net Quantity"]),
        "gross_sales": float(row["Gross Sales"]),
        "discounts": float(row["Discounts"]),
        "returns": float(row["Returns"]),
        "total_net_sales": float(row["Total Net Sales"])
    }

    return event