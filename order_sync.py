# ----------------------------------------------------------------------------- #
# orders_sync.py
#!/usr/bin/env python3
"""
Order fetch and push flows
"""
import json
import sys
from typing import Any, Dict, List
from clients import pb_login, pb_logout, pb_call, _ensure_parsed
import argparse
import logging
from datetime import datetime
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def normalize_order(raw_order: Dict[str, Any]) -> Dict[str, Any]:
    logger.debug(f"Normalizing raw order data: {raw_order.get('order_id')}")
    return {
        "order_id": raw_order.get("order_id"),
        "powerbody_order_id": raw_order.get("powerbody_order_id"),
        "status": raw_order.get("status"),
        "address1": raw_order.get("address1"),
        "address2": raw_order.get("address2", ""),
        "address3": raw_order.get("address3", ""),
        "postcode": raw_order.get("postcode", ""),
        "city": raw_order.get("city", ""),
        "county": raw_order.get("county", ""),
        "country_name": raw_order.get("country_name", ""),
        "country_code": raw_order.get("country_code", ""),
        "phone": raw_order.get("phone", ""),
        "email": raw_order.get("email", ""),
        "products": [
            {
                "product_id": item.get("product_id"),
                "sku": item.get("sku"),
                "name": item.get("name"),
                "qty": int(item.get("qty", 0)),
                "price": float(item.get("price", 0)),
                "currency": item.get("currency"),
                "tax": float(item.get("tax", 0)),
            }
            for item in raw_order.get("products", [])
        ],
    }


# Refund normalization
def normalize_refund(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "parent_id": raw.get("parent_id"),
        "refund_tax_amount": float(raw.get("refund_tax_amount", 0)),
        "subtotal": float(raw.get("subtotal", 0)),
        "refund_grand_total": float(raw.get("refund_grand_total", 0)),
        "is_refund_shipping": bool(raw.get("is_refund_shipping")),
        "refund_shipping": float(raw.get("refund_shipping", 0)),
        "items": [
            {
                "sku": it.get("sku"),
                "qty_refunded": int(it.get("qty_refunded", 0)),
                "price": float(it.get("price", 0)),
                "price_incl_tax": float(it.get("price_incl_tax", 0)),
                "row_total": float(it.get("row_total", 0)),
            }
            for it in raw.get("items", [])
        ],
    }

# Fetching orders

def fetch_orders(filter_params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    logger.info(f"Fetching orders with filter: {filter_params}")
    client, token = pb_login()
    try:
        raw = pb_call(client, token, "dropshipping.getOrders", [filter_params or {}])
        parsed = _ensure_parsed(raw)
        logger.info(f"Parsed {len(parsed)} orders")
        return [normalize_order(o) for o in parsed]
    finally:
        pb_logout(client, token)

def fetch_refund_orders(filter_params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    logger.info(f"Fetching refund orders with filter: {filter_params}")
    client, token = pb_login()
    try:
        raw = pb_call(client, token, "dropshipping.getRefundOrders", [filter_params or {}])
        parsed = _ensure_parsed(raw)
        logger.info(f"Parsed {len(parsed)} refund orders")
        return [normalize_refund(r) for r in parsed]
    finally:
        pb_logout(client, token)
# Comment helpers
def insert_comment(order_id: int, author_name: str, comment: str) -> Dict[str, Any]:
    payload = {
        "id": order_id,
        "comments": [
            {
                "author_name": author_name,
                "comment": comment,
                "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        ],
    }
    client, token = pb_login()
    try:
        resp = pb_call(client, token, "dropshipping.insertComment", [payload]) or {}
        return resp
    finally:
        pb_logout(client, token)

def get_comments() -> List[Dict[str, Any]]:
    client, token = pb_login()
    try:
        resp = pb_call(client, token, "dropshipping.getComments") or {}
        return _ensure_parsed(resp)
    finally:
        pb_logout(client, token)

# Create/update

def create_order(order: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Creating order {order.get('order_id')}")
    client, token = pb_login()
    try:
        resp = pb_call(client, token, "dropshipping.createOrder", [order]) or {}
        return {**order, "api_response": resp.get("api_response", "FAIL")}
    finally:
        pb_logout(client, token)

def update_order(order: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Updating order {order.get('order_id')}")
    client, token = pb_login()
    try:
        resp = pb_call(client, token, "dropshipping.updateOrder", [order]) or {}
        return {**order, "api_response": resp.get("api_response", "FAIL")}
    finally:
        pb_logout(client, token)

# CLI

def main() -> None:
    logger.info("Starting orders sync script")
    parser = argparse.ArgumentParser(description="Sync orders with PowerBody Dropshipping")
    subparsers = parser.add_subparsers(dest="command", required=True)

    exp_parser = subparsers.add_parser("orders-export", help="Export orders")
    exp_parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    exp_parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    exp_parser.add_argument("--ids", dest="ids", help="Comma-separated list of order IDs")

    push_parser = subparsers.add_parser("orders-push", help="Push orders")
    push_parser.add_argument("file", help="JSON file with orders to push")

    refund_parser = subparsers.add_parser("refunds-export", help="Export refund orders")
    refund_parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    refund_parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")

    comment_parser = subparsers.add_parser("comments", help="Fetch last 7‑days comments")

    args = parser.parse_args()
    logger.info(f"Command chosen: {args.command}")

    if args.command == "orders-export":
        filter_params: Dict[str, Any] = {}
        if args.from_date:
            filter_params["from"] = args.from_date
        if args.to_date:
            filter_params["to"] = args.to_date
        if args.ids:
            filter_params["ids"] = args.ids.split(",")
        orders = fetch_orders(filter_params or None)
        logger.info(f"Exporting {len(orders)} orders to stdout")
        print(json.dumps(orders, indent=2))
    elif args.command == "orders-push":
        with open(args.file, encoding="utf-8") as f:
            orders = json.load(f)
        for o in orders:
            resp = create_order(o)
            status = resp.get("api_response")
            if status == "ALREADY_EXISTS":
                resp = update_order(o)
                status = resp.get("api_response")
            logger.info(f"Order {o.get('order_id')} → {status}")
    elif args.command == "refunds-export":
        filt: Dict[str, Any] = {}
        if args.from_date:
            filt["from"] = args.from_date
        if args.to_date:
            filt["to"] = args.to_date
        refunds = fetch_refund_orders(filt or None)
        logger.info(f"Exporting {len(refunds)} refunds to stdout")
        print(json.dumps(refunds, indent=2))
    elif args.command == "comments":
        comments = get_comments()
        print(json.dumps(comments, indent=2))

if __name__ == "__main__":
    main()
