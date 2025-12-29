from flask import Flask, request
import logging
import os
import json
from datetime import datetime, timezone

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()
logging.basicConfig(level=logging.DEBUG)

# ----------------------------- GOOGLE SHEETS (tes variables) -----------------------------
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TYPE = os.environ.get("TYPE")
PROJECT_ID = os.environ.get("PROJECT_ID")
PRIVATE_KEY_ID = os.environ.get("PRIVATE_KEY_ID")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY", "").replace("\\n", "\n")
CLIENT_EMAIL = os.environ.get("CLIENT_EMAIL")
CLIENT_ID = os.environ.get("CLIENT_ID")
AUTH_URI = os.environ.get("AUTH_URI")
TOKEN_URI = os.environ.get("TOKEN_URI")
AUTH_PROVIDER_X509_CERT_URL = os.environ.get("AUTH_PROVIDER_X509_CERT_URL")
CLIENT_X509_CERT_URL = os.environ.get("CLIENT_X509_CERT_URL")

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    {
        "type": TYPE,
        "project_id": PROJECT_ID,
        "private_key_id": PRIVATE_KEY_ID,
        "private_key": PRIVATE_KEY,
        "client_email": CLIENT_EMAIL,
        "client_id": CLIENT_ID,
        "auth_uri": AUTH_URI,
        "token_uri": TOKEN_URI,
        "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
        "client_x509_cert_url": CLIENT_X509_CERT_URL,
    },
    scope,
)

client = gspread.authorize(creds)

# ----------------------------- SPREADSHEETS / ONGLET (2 onglets seulement) -----------------------------
CENTRAL_SPREADSHEET_NAME = os.environ.get("CENTRAL_SPREADSHEET_NAME", "Réponses centralisées - Publiweb")
CENTRAL_WS_REPLIES = os.environ.get("CENTRAL_WS_REPLIES", "REPLIES")
CENTRAL_WS_STOPS = os.environ.get("CENTRAL_WS_STOPS", "STOPS")

CLIENT_SPREADSHEET_NAME = os.environ.get("CLIENT_SPREADSHEET_NAME", "Réponses centralisées - Routage")
CLIENT_WS_REPLIES = os.environ.get("CLIENT_WS_REPLIES", "REPLIES")
CLIENT_WS_STOPS = os.environ.get("CLIENT_WS_STOPS", "STOPS")

_cache = {}

def get_spreadsheet(name: str):
    k = f"ss::{name}"
    if k not in _cache:
        _cache[k] = client.open(name)
    return _cache[k]

def get_worksheet(spreadsheet_name: str, worksheet_name: str):
    k = f"ws::{spreadsheet_name}::{worksheet_name}"
    if k not in _cache:
        _cache[k] = get_spreadsheet(spreadsheet_name).worksheet(worksheet_name)
    return _cache[k]

def append_row(ws, row):
    ws.append_row(row, value_input_option="USER_ENTERED")

# ----------------------------- UTILS -----------------------------
def merged_payload(req):
    data = {}
    try:
        data.update(req.args.to_dict(flat=True))
    except Exception:
        pass
    try:
        if req.form:
            data.update(req.form.to_dict(flat=True))
    except Exception:
        pass
    try:
        if req.is_json:
            body = req.get_json(silent=True) or {}
            if isinstance(body, dict):
                data.update(body)
    except Exception:
        pass
    return data

def is_stop_message(text: str) -> bool:
    t = (text or "").lower()
    return ("stop" in t) or ("36117" in (text or ""))

def build_row(route_tag: str, data: dict):
    received_at_server = datetime.now(timezone.utc).isoformat()
    text = (data.get("text") or "").strip()
    stop_flag = is_stop_message(text)

    msisdn = data.get("msisdn") or ""
    to = data.get("to") or ""
    keyword = data.get("keyword") or ""
    msg_ts = data.get("message-timestamp") or data.get("message_timestamp") or ""
    api_key = data.get("api-key") or data.get("api_key") or ""
    message_id = data.get("messageId") or data.get("message-id") or data.get("message_id") or ""

    remote_addr = request.remote_addr or ""
    user_agent = request.headers.get("User-Agent", "")

    raw_payload = json.dumps(data, ensure_ascii=False)

    # Schéma simple et stable
    row = [
        received_at_server,
        route_tag,                         # standard / client
        "TRUE" if stop_flag else "FALSE",
        msisdn,
        to,
        text,
        keyword,
        msg_ts,
        api_key,
        message_id,
        remote_addr,
        user_agent,
        raw_payload,
    ]
    return stop_flag, row

# ----------------------------- HANDLERS -----------------------------
def handle_standard(data: dict):
    stop_flag, row = build_row("standard", data)
    if stop_flag:
        ws = get_worksheet(CENTRAL_SPREADSHEET_NAME, CENTRAL_WS_STOPS)
    else:
        ws = get_worksheet(CENTRAL_SPREADSHEET_NAME, CENTRAL_WS_REPLIES)
    append_row(ws, row)

def handle_client(data: dict):
    stop_flag, row = build_row("client", data)
    if stop_flag:
        ws = get_worksheet(CLIENT_SPREADSHEET_NAME, CLIENT_WS_STOPS)
    else:
        ws = get_worksheet(CLIENT_SPREADSHEET_NAME, CLIENT_WS_REPLIES)
    append_row(ws, row)

def handle_inbound(kind: str):
    logging.debug(f"Received inbound SMS ({kind})")
    logging.debug(f"Request content type: {request.content_type}")
    logging.debug(f"Request body: {request.get_data(as_text=True)}")

    data = merged_payload(request)
    if not data:
        return ("Requête invalide", 400)

    try:
        if kind == "standard":
            handle_standard(data)
        else:
            handle_client(data)
        return ("", 204)
    except Exception as e:
        logging.exception(f"Failed to handle inbound ({kind}): {e}")
        # 204 pour éviter retries/doublons
        return ("", 204)

# ----------------------------- ROUTES -----------------------------
@app.route("/webhooks/inbound-sms", methods=["GET", "POST"])
def inbound_sms_standard():
    return handle_inbound("standard")

@app.route("/webhooks/inbound-sms-client", methods=["GET", "POST"])
def inbound_sms_client():
    return handle_inbound("client")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
