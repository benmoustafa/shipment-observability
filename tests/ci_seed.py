"""
tests/ci_seed.py

CI database seeder for GitHub Actions.
Inserts a lightweight sample dataset into MySQL raw_shipments so dbt run
and dbt test can execute cleanly in CI without requiring the 95MB CSV dataset.
"""

import os
import pandas as pd
from sqlalchemy import create_engine

def seed_ci_database():
    db_user = os.environ.get("DB_USER", "root")
    db_pass = os.environ.get("DB_PASSWORD", "Ben.2003!")
    db_host = os.environ.get("DB_HOST", "127.0.0.1")
    db_port = os.environ.get("DB_PORT", "3306")
    db_name = os.environ.get("DB_NAME", "shipment_observability")

    uri = f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(uri)

    n_rows = 50
    df = pd.DataFrame({
        "Order Id": list(range(1001, 1001 + n_rows)),
        "Order Customer Id": [200 + i for i in range(n_rows)],
        "Order Item Id": list(range(5001, 5001 + n_rows)),
        "order date (DateOrders)": ["01/01/2017 12:00"] * n_rows,
        "shipping date (DateOrders)": ["01/03/2017 12:00"] * n_rows,
        "Days for shipping (real)": [3] * n_rows,
        "Days for shipment (scheduled)": [4] * n_rows,
        "Delivery Status": ["Shipping on time"] * n_rows,
        "Late_delivery_risk": [0] * n_rows,
        "Shipping Mode": ["Standard Class"] * n_rows,
        "Type": ["DEBIT"] * n_rows,
        "Order Status": ["COMPLETE"] * n_rows,
        "Sales": [150.0] * n_rows,
        "Order Item Total": [150.0] * n_rows,
        "Order Profit Per Order": [25.0] * n_rows,
        "Benefit per order": [25.0] * n_rows,
        "Order Item Discount": [0.0] * n_rows,
        "Order Item Discount Rate": [0.0] * n_rows,
        "Order Item Product Price": [150.0] * n_rows,
        "Order Item Profit Ratio": [0.16] * n_rows,
        "Order Item Quantity": [1] * n_rows,
        "Sales per customer": [150.0] * n_rows,
        "Customer Id": [200 + i for i in range(n_rows)],
        "Customer Fname": ["John"] * n_rows,
        "Customer Lname": ["Doe"] * n_rows,
        "Customer Email": ["john.doe@example.com"] * n_rows,
        "Customer Segment": ["Consumer"] * n_rows,
        "Customer City": ["New York"] * n_rows,
        "Customer State": ["NY"] * n_rows,
        "Customer Country": ["EE. UU."] * n_rows,
        "Customer Street": ["5th Ave"] * n_rows,
        "Customer Zipcode": [10001] * n_rows,
        "Product Card Id": [900 + (i % 5) for i in range(n_rows)],
        "Product Name": ["Field Sport Cleats"] * n_rows,
        "Product Price": [150.0] * n_rows,
        "Product Status": [0] * n_rows,
        "Category Id": [17] * n_rows,
        "Category Name": ["Cleats"] * n_rows,
        "Department Id": [4] * n_rows,
        "Department Name": ["Apparel"] * n_rows,
        "Market": ["USCA"] * n_rows,
        "Order Region": ["East of US"] * n_rows,
        "Order Country": ["Estados Unidos"] * n_rows,
        "Order State": ["NY"] * n_rows,
        "Order City": ["New York"] * n_rows,
        "Latitude": [40.7128] * n_rows,
        "Longitude": [-74.0060] * n_rows,
    })

    print(f"Seeding {len(df)} sample rows to MySQL raw_shipments table...")
    df.to_sql("raw_shipments", con=engine, if_exists="replace", index=False)
    print("CI database seed complete.")

if __name__ == "__main__":
    seed_ci_database()
