from zoneinfo import ZoneInfo
from datetime import datetime
import pandas as pd
from itertools import cycle

df2 = pd.read_csv("business.retailsales2.csv")
df2.columns = df2.columns.str.strip()

rows2 = cycle(df2.to_dict(orient="records"))

def send_retail_sales_data2():
    row = next(rows2)

    event = {
        "source": "monthly_sales",
        "event_id": f"M{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        "event_timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "month": row["Month"],
        "year": int(row["Year"]),
        "total_orders": int(row["Total Orders"]),
        "gross_sales": float(row["Gross Sales"]),
        "discounts": float(row["Discounts"]),
        "returns": float(row["Returns"]),
        "net_sales": float(row["Net Sales"]),
        "shipping": float(row["Shipping"]),
        "total_sales": float(row["Total Sales"])
    }

    return event