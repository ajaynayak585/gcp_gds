import logging
import os
import functions_framework
from flask import jsonify, make_response, request

from utils.order_utils import validate_payload, enrich_payload, simulate_db_save

# ——— Logger setup ———
logger = logging.getLogger('order_service')
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))
    logger.addHandler(h)


@functions_framework.http
def order_event(request):
    """
    HTTP Cloud Run Function to handle 'order.created' events.
    Expects a rich JSON payload (see utils/order_utils.validate_payload).
    """

    logger.info("Received %s %s", request.method, request.path)
    if request.method != "POST":
        logger.warning("Only POST allowed")
        return make_response(jsonify({"error": "Use POST"}), 405)

    data = request.get_json(silent=True)
    if not data:
        logger.error("No JSON body")
        return make_response(jsonify({"error": "Invalid JSON"}), 400)

    # 1) Validate
    try:
        validate_payload(data)
    except ValueError as e:
        logger.error("Validation failed: %s", e)
        return make_response(jsonify({"error": str(e)}), 400)

    # 2) Enrich
    enriched = enrich_payload(data)

    # 3) “Save” (fake)
    try:
        if not simulate_db_save(enriched):
            raise RuntimeError("DB save returned False")
    except Exception:
        logger.exception("Error saving to DB")
        return make_response(jsonify({"error": "Internal error"}), 500)

    # 4) Build response
    resp = {
        "status":           "processed",
        "order_id":         enriched["order_id"],
        "processing_id":    enriched["processing_id"],
        "processed_at":     enriched["processed_at"],
        "items_count":      len(enriched["items"]),
        "total_amount":     enriched["total_amount"],
        "payment_method":   enriched["payment_method"],
        "shipping_address": enriched["shipping_address"],
        "message":          "Order received and stored."
    }
    logger.info("Order %s processed successfully", enriched["order_id"])
    return make_response(jsonify(resp), 200)