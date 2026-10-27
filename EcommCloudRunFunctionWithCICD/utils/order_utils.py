import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger('order_utils')


def validate_payload(data: Dict[str, Any]):
    """
    Ensure required fields are present and types look sane.
    Raises ValueError if something’s missing or malformed.
    """
    required = [
        "order_id",
        "customer_id",
        "items",
        "order_date",
        "shipping_address",
        "payment_method",
        "total_amount"
    ]
    missing = [f for f in required if f not in data]
    if missing:
        raise ValueError(f"Missing fields: {missing}")

    # items
    items = data["items"]
    if not isinstance(items, list) or len(items) == 0:
        raise ValueError("`items` must be a non-empty list")
    for i, it in enumerate(items):
        for key in ("sku", "name", "qty", "unit_price"):
            if key not in it:
                raise ValueError(f"Item[{i}] missing '{key}'")
        if not isinstance(it["qty"], int) or it["qty"] <= 0:
            raise ValueError(f"Item[{i}].qty must be a positive integer")
        if not isinstance(it["unit_price"], (int, float)) or it["unit_price"] < 0:
            raise ValueError(f"Item[{i}].unit_price must be non-negative number")

    # shipping_address
    addr = data["shipping_address"]
    if not isinstance(addr, dict):
        raise ValueError("`shipping_address` must be an object")
    for field in ("line1", "city", "state", "postal_code", "country"):
        if field not in addr:
            raise ValueError(f"`shipping_address` missing '{field}'")

    # payment_method
    if not isinstance(data["payment_method"], str):
        raise ValueError("`payment_method` must be a string")

    # total_amount
    total_amount = data["total_amount"]
    if not isinstance(total_amount, (int, float)):
        raise ValueError("`total_amount` must be a number")

    # verify sum
    computed = sum(it["qty"] * it["unit_price"] for it in items)
    if round(computed, 2) != round(total_amount, 2):
        raise ValueError(
            f"total_amount mismatch: expected {computed:.2f}, got {total_amount:.2f}"
        )

    logger.info("Payload validated for order %s", data["order_id"])
    return True


def enrich_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add processing metadata: a unique processing ID and timestamp.
    """
    data["processing_id"] = uuid.uuid4().hex
    data["processed_at"] = datetime.utcnow().isoformat() + "Z"
    logger.info("Enriched payload: processing_id=%s", data["processing_id"])
    return data


def simulate_db_save(data: Dict[str, Any]) -> bool:
    """
    Fake function to “save” the order to a database.
    """
    logger.info("Simulating DB save for order %s", data["order_id"])
    # …imagine inserting into Cloud SQL or Firestore…
    return True