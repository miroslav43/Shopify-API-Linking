# clients.py
#!/usr/bin/env python3
"""
Common Powerbody and Shopify client wrappers
"""
from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Tuple
import requests
from requests.auth import HTTPBasicAuth
from zeep import Client, Settings
from zeep.transports import Transport
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Load env
from dotenv import load_dotenv
load_dotenv()

# Optional Shopify API key/secret (for Basic Auth fallback)
REQ = {
    "POWERBODY_USER": os.getenv("POWERBODY_USER"),
    "POWERBODY_PASS": os.getenv("POWERBODY_PASS"),
    "POWERBODY_WSDL": os.getenv(
        "POWERBODY_WSDL",
        "https://www.powerbody.co.uk/index.php/api/soap/?wsdl"
    ),
    "SHOPIFY_STORE": os.getenv("SHOPIFY_STORE"),
    "SHOPIFY_TOKEN": os.getenv("SHOPIFY_TOKEN"),
    "SHOPIFY_LOCATION_ID": os.getenv("SHOPIFY_LOCATION_ID"),
}

# Optional Shopify API key/secret (for Basic Auth fallback)
API_KEY = os.getenv("SHOPIFY_API_KEY")
API_SECRET = os.getenv("SHOPIFY_API_SECRET")

# Ensure core credentials present
required = ["POWERBODY_USER", "POWERBODY_PASS", "POWERBODY_WSDL", "SHOPIFY_STORE", "SHOPIFY_LOCATION_ID"]
missing = [k for k in required if not REQ.get(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

# Must have either an access token or both API key & secret
if not REQ.get("SHOPIFY_TOKEN") and not (API_KEY and API_SECRET):
    raise RuntimeError(
        "Missing Shopify auth: set SHOPIFY_TOKEN or both SHOPIFY_API_KEY & SHOPIFY_API_SECRET"
    )

# Powerbody SOAP setup
ZE_SETTINGS = Settings(strict=False, xml_huge_tree=True)
ZE_TRANSPORT = Transport(timeout=60)

def pb_login() -> Tuple[Client, str]:
    logger.info(f"Logging in to PowerBody SOAP API at WSDL {REQ['POWERBODY_WSDL']}")
    client = Client(REQ["POWERBODY_WSDL"], settings=ZE_SETTINGS, transport=ZE_TRANSPORT)
    token = client.service.login(REQ["POWERBODY_USER"], REQ["POWERBODY_PASS"])
    return client, token

def pb_logout(client: Client, token: str) -> None:
    logger.info(f"Logging out of PowerBody session token {token}")
    try:
        client.service.endSession(token)
    except Exception:
        pass

def pb_call(client: Client, token: str, resource: str, params: list[Any] | None = None) -> Any:
    logger.debug(f"Calling resource '{resource}' with params {params}")
    return client.service.call(token, resource, params or [])

def _ensure_parsed(payload: Any) -> Any:
    logger.debug(f"Ensuring payload parsed: {type(payload).__name__}")
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return []
    return payload or []

# Shopify REST/GraphQL setup
SHOPIFY_REST = f"https://{REQ['SHOPIFY_STORE']}/admin/api/2025-04"
GRAPHQL_URL = f"{SHOPIFY_REST}/graphql.json"
shop = requests.Session()
if REQ.get("SHOPIFY_TOKEN"):
    # Bearer token auth
    shop.headers.update({
        "X-Shopify-Access-Token": REQ["SHOPIFY_TOKEN"],
        "Content-Type": "application/json",
        "User-Agent": "PowerBodyDropshipSync/4.0",
    })
else:
    # Basic auth fallback
    shop.auth = HTTPBasicAuth(API_KEY, API_SECRET)

def gql(query: str, variables: dict[str, Any] | None = None) -> Any:
    r = shop.post(GRAPHQL_URL, json={"query": query, "variables": variables or {}}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def gid_num(gid: str) -> int:
    return int(gid.rsplit("/", 1)[-1])