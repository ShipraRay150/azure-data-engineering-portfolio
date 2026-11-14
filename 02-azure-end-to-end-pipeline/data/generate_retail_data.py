"""Generate deterministic synthetic retail orders for portfolio demonstrations."""
from pathlib import Path
import argparse
import pandas as pd


def build_orders(row_count: int = 500) -> pd.DataFrame:
    products = [
        ("P100", "Laptop", "Electronics", 799.00),
        ("P200", "Headphones", "Electronics", 89.00),
        ("P300", "Desk Chair", "Furniture", 159.00),
        ("P400", "Coffee Maker", "Home", 59.00),
        ("P500", "Running Shoes", "Sports", 74.00),
    ]
    rows = []
    for i in range(1, row_count + 1):
        product_id, product_name, category, unit_price = products[i % len(products)]
        quantity = (i % 4) + 1
        rows.append({
            "order_id": f"ORD{i:06d}",
            "customer_id": f"CUST{(i % 120) + 1:04d}",
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "order_timestamp": pd.Timestamp("2026-01-01") + pd.Timedelta(hours=i * 3),
            "country": ["India", "UK", "USA", "Canada"][i % 4],
            "status": ["COMPLETED", "SHIPPED", "PROCESSING"][i % 3],
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--output", default="data/generated/orders")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    df = build_orders(args.rows)
    # Multiple files simulate incremental arrivals.
    for batch, start in enumerate(range(0, len(df), 100), start=1):
        df.iloc[start:start + 100].to_csv(output / f"orders_batch_{batch:03d}.csv", index=False)
    print(f"Generated {len(df)} synthetic orders in {output}")


if __name__ == "__main__":
    main()
