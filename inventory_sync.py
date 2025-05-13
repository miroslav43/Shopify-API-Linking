# ----------------------------------------------------------------------------- #
# inventory_sync.py
#!/usr/bin/env python3
"""
Inventory synchronization flow
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import csv
import os
import sys
from typing import Any, Dict, List
import requests

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

from clients import (
    pb_login, pb_logout, pb_call, _ensure_parsed,
    gql, gid_num, shop, SHOPIFY_REST, REQ
)

CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))

# Fetch products

def fetch_products() -> List[Dict[str, Any]]:
    logger.info("Fetching product list from PowerBody API...")
    client, token = pb_login()
    try:
        basic = _ensure_parsed(pb_call(client, token, "dropshipping.getProductList"))
        logger.debug(f"Received {len(basic)} basic product entries")
        out: List[Dict[str, Any]] = []
        for item in basic:
            pid = int(item.get("product_id", 0))
            info = _ensure_parsed(pb_call(client, token, "dropshipping.getProductInfo", [pid])) or {}
            status = (info.get("status") or "").lower()
            if status not in {"active", "out of stock"}:
                continue
            out.append({
                "sku": item.get("sku", ""),
                "price": float(item.get("price", 0)),
                "weight": float(info.get("weight") or 0),
                "qty": int(item.get("qty", 0)),
                "name": info.get("name", item.get("sku", "")),
                "description": info.get("description_en", ""),
                "description_en": info.get("description_en", ""),
                "description_pl": info.get("description_pl", ""),
                "brand": info.get("manufacturer", ""),
                "trade_price": float(info.get("trade_price", 0)),
                "url": info.get("url", ""),
                "image": info.get("image", ""),
                "status": info.get("status", ""),
                "detail_price": float(info.get("detail_price", 0)),
                "vat_rate": float(info.get("vat_rate", 0)),
                "save": float(info.get("save", 0)),
                "save_percent": float(info.get("save_percent", 0)),
                "category": info.get("category", ""),
                "portion_count": int(info.get("portion_count", 0)),
                "price_per_serving": float(info.get("price_per_serving", 0)),
            })
        return out
    finally:
        pb_logout(client, token)

# Shopify variant lookup and upsert

def find_variant(sku: str) -> dict | None:
    logger.debug(f"Searching Shopify for variant with SKU '{sku}'")
    q = "query($q:String!){ productVariants(first:1,query:$q){edges{node{id sku inventoryItem{id}}}} }"
    resp = gql(q, {"q": f"sku:{sku}"})
    logger.debug(f"Shopify variant search response: {resp}")
    edges = resp["productVariants"]["edges"]
    return edges[0]["node"] if edges else None


def create_product(p: Dict[str, Any]) -> None:
    logger.info(f"Creating Shopify product for SKU {p['sku']}")
    body = {"product": {
        "title": p["name"],
        "body_html": p["description"],
        "vendor": p["brand"],
        "status": "active",
        "variants": [{
            "sku": p["sku"],
            "price": p["price"],
            "inventory_management": "SHOPIFY",
            "inventory_policy": "continue",
            "weight": p["weight"],
            "weight_unit": "g",
        }],
        "images": ([{"src": p["image"]}] if p.get("image") else []),
    }}
    r = shop.post(f"{SHOPIFY_REST}/products.json", json=body, timeout=30)
    r.raise_for_status()


def update_variant(variant_gid: str, p: Dict[str, Any]) -> None:
    vid = gid_num(variant_gid)
    logger.info(f"Updating Shopify variant ID {vid} (SKU {p['sku']})")
    body = {"variant": {
        "id": vid,
        "price": p["price"],
        "weight": p["weight"],
        "sku": p["sku"],
    }}
    r = shop.put(f"{SHOPIFY_REST}/variants/{vid}.json", json=body, timeout=30)
    r.raise_for_status()


def set_inventory(item_gid: str, qty: int) -> None:
    iid = gid_num(item_gid)
    try:
        loc_id_int = int(REQ["SHOPIFY_LOCATION_ID"])
    except (ValueError, TypeError):
        # auto‑detect first location and cache it in REQ
        q = "query{ locations(first:1){edges{node{id legacyResourceId}}} }"
        loc_resp = gql(q)
        node = loc_resp["locations"]["edges"][0]["node"]
        loc_id_int = int(node["legacyResourceId"])
        REQ["SHOPIFY_LOCATION_ID"] = str(loc_id_int)
        logger.warning(f"SHOPIFY_LOCATION_ID was invalid; using detected location {loc_id_int}")
    logger.info(f"Setting inventory for item ID {iid} at location {loc_id_int} to {qty}")
    body = {
        "location_id": loc_id_int,
        "inventory_item_id": iid,
        "available": qty,
    }
    # First try classic REST call
    try:
        r = shop.post(f"{SHOPIFY_REST}/inventory_levels/set.json", json=body, timeout=30)
        r.raise_for_status()
        return
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.warning("REST inventory_levels/set returned 404 – falling back to GraphQL inventorySetOnHandQuantities")
            mutation = """
            mutation inventorySetOnHand($input: InventorySetOnHandQuantitiesInput!) {
              inventorySetOnHandQuantities(input: $input) {
                userErrors { field message }
                inventoryAdjustmentGroup { createdAt }
              }
            }
            """
            loc_gid = f"gid://shopify/Location/{loc_id_int}"
            input_obj = {
                "reason": "correction",
                "setQuantities": [
                    {
                        "inventoryItemId": item_gid,
                        "locationId": loc_gid,
                        "quantity": qty,
                    }
                ]
            }
            resp = gql(mutation, {"input": input_obj})
            errors = resp["inventorySetOnHandQuantities"]["userErrors"]
            if errors:
                raise RuntimeError(errors)
            return
        raise

# Worker flows

def upsert(p: Dict[str, Any]) -> str:
    logger.debug(f"Upsert flow for SKU {p['sku']}")
    v = find_variant(p["sku"])
    if v:
        update_variant(v["id"], p)
        set_inventory(v["inventoryItem"]["id"], p["qty"])
        return "updated"
    create_product(p)
    v2 = find_variant(p["sku"])
    if v2:
        set_inventory(v2["inventoryItem"]["id"], p["qty"])
    return "created"


def only_inventory(p: Dict[str, Any]) -> str:
    logger.debug(f"Inventory-only flow for SKU {p['sku']}")
    v = find_variant(p["sku"])
    if not v:
        return "missing"
    set_inventory(v["inventoryItem"]["id"], p["qty"])
    return "inventory"

# CSV export

def export_csv(records: List[Dict[str, Any]], prefix: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"powerbody_{prefix}_{ts}.csv"
    logger.info(f"Exporting CSV with {len(records)} records to '{fname}'")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=records[0].keys())
        w.writeheader()
        w.writerows(records)
    return fname

# CLI

def main() -> None:
    logger.info("Starting inventory sync script")
    if len(sys.argv) != 2 or sys.argv[1] not in {"seed", "inventory"}:
        print("Usage: python inventory_sync.py [seed|inventory]")
        sys.exit(1)
    mode = sys.argv[1]
    products = fetch_products()
    logger.info(f"Mode selected: {mode}; fetched {len(products)} products")
    print(f"Got {len(products)} products")
    print(f"Exported to {export_csv(products, 'inventory')}")
    worker = upsert if mode == "seed" else only_inventory
    status_map = {"created": "🟢", "updated": "🔄", "inventory": "📦", "missing": "⚠️"}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(worker, p): p["sku"] for p in products}
        for fut in as_completed(futures):
            sku = futures[fut]
            try:
                res = fut.result()
                print(f"{status_map.get(res, '?')} {sku} → {res}")
            except Exception as e:
                print(f"❌ {sku} → {e}")
    print(f"\n✅ {mode.capitalize()} complete.")

if __name__ == "__main__":
    main()
