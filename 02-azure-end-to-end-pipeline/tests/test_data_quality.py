"""Local tests for project-level quality rules (no Spark session required)."""
import pandas as pd


def validate_order(row):
    if not row.get("order_id"):
        return "missing_order_id"
    if not row.get("customer_id"):
        return "missing_customer_id"
    if row.get("quantity") is None or row["quantity"] <= 0:
        return "invalid_quantity"
    if row.get("unit_price") is None or row["unit_price"] < 0:
        return "invalid_unit_price"
    if not row.get("order_timestamp"):
        return "missing_order_timestamp"
    return None


def test_valid_order_passes():
    row = {
        "order_id": "ORD000001", "customer_id": "CUST0001",
        "quantity": 2, "unit_price": 10.0, "order_timestamp": "2026-01-01T10:00:00"
    }
    assert validate_order(row) is None


def test_missing_order_id_fails():
    row = {
        "order_id": "", "customer_id": "CUST0001",
        "quantity": 2, "unit_price": 10.0, "order_timestamp": "2026-01-01T10:00:00"
    }
    assert validate_order(row) == "missing_order_id"


def test_invalid_quantity_fails():
    row = {
        "order_id": "ORD000001", "customer_id": "CUST0001",
        "quantity": 0, "unit_price": 10.0, "order_timestamp": "2026-01-01T10:00:00"
    }
    assert validate_order(row) == "invalid_quantity"
