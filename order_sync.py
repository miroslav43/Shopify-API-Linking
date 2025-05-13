# ----------------------------------------------------------------------------- #
# orders_sync.py
#!/usr/bin/env python3
"""
Order fetch and push flows
"""
import json
import sys
from typing import Any, Dict, List
from clients import (
    pb_login, pb_logout, pb_call, _ensure_parsed,
    shop, SHOPIFY_REST
)
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
        json_payload = json.dumps(filter_params or {})
        raw = pb_call(client, token, "dropshipping.getOrders", [json_payload])
        parsed = _ensure_parsed(raw)
        logger.info(f"Parsed {len(parsed)} orders")
        return [normalize_order(o) for o in parsed]
    finally:
        pb_logout(client, token)

def fetch_refund_orders(filter_params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    logger.info(f"Fetching refund orders with filter: {filter_params}")
    client, token = pb_login()
    try:
        json_payload = json.dumps(filter_params or {})
        raw = pb_call(client, token, "dropshipping.getRefundOrders", [json_payload])
        parsed = _ensure_parsed(raw)
        logger.info(f"Parsed {len(parsed)} refund orders")
        return [normalize_refund(r) for r in parsed]
    finally:
        pb_logout(client, token)


# New: fetch product list
# ---------------------------------------------------------------------------#
# Shopify helpers

def fetch_product_list() -> List[Dict[str, Any]]:
    """
    Fetch the full product list from PowerBody Dropshipping.
    Wrapper around the `dropshipping.getProductList` endpoint.
    """
    logger.info("Fetching product list from PowerBody API")
    client, token = pb_login()
    try:
        raw = pb_call(client, token, "dropshipping.getProductList")
        parsed = _ensure_parsed(raw)
        logger.info(f"Fetched {len(parsed)} products")
        return parsed
    finally:
        pb_logout(client, token)


def fetch_shopify_orders(created_at_min: str | None = None,
                         created_at_max: str | None = None,
                         status: str = "any") -> List[Dict[str, Any]]:
    """
    Pull orders from Shopify REST.
    * `status` can be 'open', 'closed', 'cancelled', or 'any'.
    * Date strings are YYYY‑MM‑DD (UTC). If omitted, fetches all.
    Returns the raw Shopify order JSON objects.
    """
    logger.info(f"Fetching Shopify orders status={status} "
                f"from={created_at_min} to={created_at_max}")
    params = {"status": status, "limit": 250, "order": "created_at asc"}
    if created_at_min:
        params["created_at_min"] = f"{created_at_min}T00:00:00Z"
    if created_at_max:
        params["created_at_max"] = f"{created_at_max}T23:59:59Z"
    out: List[Dict[str, Any]] = []
    page_info = None
    while True:
        # pagination cursor
        if page_info:
            params["page_info"] = page_info
        r = shop.get(f"{SHOPIFY_REST}/orders.json", params=params, timeout=30)
        r.raise_for_status()
        batch = r.json().get("orders", [])
        out.extend(batch)
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        page_info = link.split("page_info=")[1].split(">")[0]
    logger.info(f"Fetched {len(out)} Shopify orders")
    return out

def export_csv(records: List[Dict[str, Any]], fname: str) -> str:
    """
    Simple CSV export util; writes the list of dictionaries to *fname*.
    Returns the file name.
    """
    import csv
    logger.info(f"Exporting {len(records)} rows to CSV '{fname}'")
    if not records:
        logger.warning("No records – CSV will contain only headers")
        hdr = []
    else:
        hdr = records[0].keys()
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        w.writerows(records)
    return fname

def shopify_order_to_powerbody(o: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Shopify order JSON into a payload accepted by dropshipping.createOrder.
    """
    def map_address(addr: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": addr.get("first_name", ""),
            "surname": addr.get("last_name", ""),
            "address1": addr.get("address1") or "",
            "address2": addr.get("address2") or "",
            "address3": "",
            "postcode": addr.get("zip") or "",
            "city": addr.get("city") or "",
            "county": addr.get("province") or "",
            "country_name": addr.get("country") or "",
            "country_code": addr.get("country_code") or "",
            "phone": addr.get("phone") or "",
            "email": o.get("email") or "",
        }
    line_items = []
    for li in o.get("line_items", []):
        line_items.append({
            # product_id is optional – leave blank
            "sku": li.get("sku") or "",
            "name": li.get("name") or "",
            "qty": int(li.get("quantity", 0)),
            "price": float(li.get("price", 0)),
            "currency": o.get("currency") or "EUR",
            "tax": float(sum(t["rate"] for t in li.get("tax_lines", []))) * 100
                    if li.get("tax_lines") else 0.0,
        })
    shipping_lines = o.get("shipping_lines", [])
    shipping_price = float(shipping_lines[0]["price"]) if shipping_lines else 0.0
    transport_code = shipping_lines[0].get("title") if shipping_lines else ""
    total_weight = float(o.get("total_weight", 0)) / 1000.0  # g → kg
    return {
        "id": o.get("name") or str(o.get("id")),   # use Shopify order name (#1003)
        "status": "on hold",
        "currency_rate": 1,
        "transport_code": transport_code,
        "weight": total_weight,
        "date_add": o.get("created_at", "")[:10],
        "comment": o.get("note") or "",
        "shipping_price": shipping_price,
        "address": map_address(o.get("shipping_address") or o.get("billing_address") or {}),
        "products": line_items,
    }

def sync_shopify_to_powerbody(created_at_min: str | None = None,
                              created_at_max: str | None = None) -> None:
    """
    Fetch Shopify orders (open/unfulfilled) and push them to PowerBody.
    """
    orders = fetch_shopify_orders(created_at_min, created_at_max, status="open")
    logger.info(f"Syncing {len(orders)} Shopify orders to PowerBody")
    for so in orders:
        pb_payload = shopify_order_to_powerbody(so)
        resp = create_order(pb_payload)
        status = resp.get("api_response")
        if status == "ALREADY_EXISTS":
            resp = update_order(pb_payload)
            status = resp.get("api_response")
        logger.info(f"Shopify order {so.get('name')} → PowerBody {status}")
# Comment helpers
def insert_comment(order_id: int, author_name: str, comment: str) -> Dict[str, Any]:
    payload = {
        "id": order_id,
        "comments": [
            {
                "author_name": author_name,
                "comments": comment,
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
        resp = pb_call(client, token, "dropshipping.createOrder", [json.dumps(order)]) or {}
        return {**order, "api_response": resp.get("api_response", "FAIL")}
    finally:
        pb_logout(client, token)

def update_order(order: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Updating order {order.get('order_id')}")
    client, token = pb_login()
    try:
        resp = pb_call(client, token, "dropshipping.updateOrder", [json.dumps(order)]) or {}
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

    product_parser = subparsers.add_parser("products", help="Export product list")

    shop_exp_parser = subparsers.add_parser("shopify-export", help="Export orders from Shopify")
    shop_exp_parser.add_argument("--from", dest="from_date", help="Start date (YYYY‑MM‑DD)")
    shop_exp_parser.add_argument("--to", dest="to_date", help="End date (YYYY‑MM‑DD)")
    shop_exp_parser.add_argument("--csv", dest="csv_path", help="Write output to CSV instead of JSON")

    shop_sync_parser = subparsers.add_parser("shopify-sync",
                                             help="Fetch Shopify orders and push to PowerBody")
    shop_sync_parser.add_argument("--from", dest="from_date", help="Start date (YYYY‑MM‑DD)")
    shop_sync_parser.add_argument("--to", dest="to_date", help="End date (YYYY‑MM‑DD)")

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
    elif args.command == "products":
        products = fetch_product_list()
        logger.info(f"Exporting {len(products)} products to stdout")
        print(json.dumps(products, indent=2))
    elif args.command == "shopify-export":
        sorders = fetch_shopify_orders(args.from_date, args.to_date)
        logger.info(f"Fetched {len(sorders)} Shopify orders")
        if args.csv_path:
            export_csv(sorders, args.csv_path)
            print(f"✅ CSV written to {args.csv_path}")
        else:
            print(json.dumps(sorders, indent=2))
    elif args.command == "shopify-sync":
        sync_shopify_to_powerbody(args.from_date, args.to_date)
    elif args.command == "comments":
        comments = get_comments()
        print(json.dumps(comments, indent=2))

if __name__ == "__main__":
    main()
