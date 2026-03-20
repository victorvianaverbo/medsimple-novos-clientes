"""
fetch_hotmart.py
Busca todo o histórico de vendas aprovadas na Hotmart desde o início.
Salva em .tmp/hotmart_raw.csv

Campos extraídos por venda:
  email, telefone, nome, data_compra, plataforma
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

HOTMART_BASIC = os.getenv("HOTMART_BASIC")
AUTH_URL = "https://api-sec-vlc.hotmart.com/security/oauth/token"
SALES_URL = "https://developers.hotmart.com/payments/api/v1/sales/history"

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "hotmart_raw.csv")

# Status que indicam pagamento confirmado
APPROVED_STATUSES = ["APPROVED", "COMPLETE"]


def get_access_token():
    """Autentica via OAuth2 client_credentials e retorna o access_token."""
    resp = requests.post(
        AUTH_URL,
        headers={
            "Authorization": HOTMART_BASIC,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"Token não retornado. Resposta: {resp.json()}")
    print(f"[Hotmart] Token obtido com sucesso.")
    return token


def ms_to_datetime(ms):
    """Converte timestamp em milissegundos para string ISO 8601."""
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_all_sales(token):
    """
    Busca todas as vendas aprovadas paginando até o fim.
    Retorna lista de dicts com campos normalizados.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    all_sales = []
    page_token = None
    page_num = 1

    while True:
        params = {
            "max_results": 500,
            "transaction_status": ",".join(APPROVED_STATUSES),
        }
        if page_token:
            params["page_token"] = page_token

        print(f"[Hotmart] Buscando página {page_num}... (total até agora: {len(all_sales)})")

        resp = requests.get(SALES_URL, headers=headers, params=params, timeout=60)

        if resp.status_code == 429:
            print("[Hotmart] Rate limit atingido. Aguardando 30s...")
            time.sleep(30)
            continue

        if resp.status_code == 401:
            raise ValueError("[Hotmart] Token expirado ou inválido. Re-autentique.")

        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        if not items:
            print("[Hotmart] Nenhum item nesta página. Fim da paginação.")
            break

        for item in items:
            buyer = item.get("buyer", {}) or {}
            purchase = item.get("purchase", {}) or {}

            # Tentar extrair telefone de campos disponíveis
            phone = (
                buyer.get("phone")
                or buyer.get("phone_number")
                or buyer.get("document", "")  # document às vezes é CPF/telefone
            )
            # Se o documento parece CPF (11 dígitos de número), não é telefone
            if phone and str(phone).replace(".", "").replace("-", "").isdigit():
                digits = "".join(filter(str.isdigit, str(phone)))
                if len(digits) == 11 and digits.startswith("0"):
                    phone = digits  # provavelmente celular
                elif len(digits) == 11 and not digits[:2].startswith("0"):
                    phone = ""  # provavelmente CPF

            # Data da compra (approved_date em ms)
            date_ms = (
                purchase.get("approved_date")
                or purchase.get("order_date")
                or purchase.get("date")
            )

            all_sales.append({
                "email": (buyer.get("email") or "").strip().lower(),
                "telefone": "".join(filter(str.isdigit, str(phone or ""))),
                "nome": (buyer.get("name") or "").strip(),
                "data_compra": ms_to_datetime(date_ms),
                "plataforma": "hotmart",
            })

        # Verificar próxima página
        page_info = data.get("page_info", {}) or {}
        next_token = page_info.get("next_page_token")
        if not next_token:
            print("[Hotmart] Fim da paginação.")
            break

        page_token = next_token
        page_num += 1
        time.sleep(0.5)  # respeitar rate limit

    return all_sales


def save_to_csv(sales, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = ["email", "telefone", "nome", "data_compra", "plataforma"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sales)
    print(f"[Hotmart] {len(sales)} vendas salvas em {output_path}")


def main():
    print("=" * 50)
    print("HOTMART — Buscando histórico completo de vendas")
    print("=" * 50)

    token = get_access_token()
    sales = fetch_all_sales(token)
    save_to_csv(sales, OUTPUT_FILE)

    print(f"\n[Hotmart] Concluído! Total de vendas: {len(sales)}")


if __name__ == "__main__":
    main()
