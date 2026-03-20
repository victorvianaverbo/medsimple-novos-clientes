"""
fetch_guru.py
Busca todo o histórico de transações e assinaturas aprovadas na Guru (Digital Manager Guru).
Salva em .tmp/guru_raw.csv

Campos extraídos:
  email, telefone, nome, data_compra, plataforma

Self-annealing: testa variações de endpoint automaticamente se necessário.
"""

import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

GURU_TOKEN = os.getenv("GURU_TOKEN")

# Possíveis endpoints base — o script testa em ordem até encontrar o correto
CANDIDATE_BASE_URLS = [
    "https://api.digitalmanager.guru/v2",
    "https://api.digitalmanager.guru/v1",
    "https://api.digitalmanager.guru",
]

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "guru_raw.csv")

HEADERS = {
    "Authorization": f"Bearer {GURU_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Status de transações que indicam pagamento confirmado
PAID_STATUSES = {"paid", "approved", "complete", "active", "confirmed"}


def discover_base_url():
    """
    Testa endpoints candidatos para encontrar a URL base da API Guru.
    Retorna a URL base funcional.
    """
    test_paths = ["/transactions", "/sales", "/orders"]

    for base in CANDIDATE_BASE_URLS:
        for path in test_paths:
            url = f"{base}{path}"
            try:
                resp = requests.get(url, headers=HEADERS, params={"per_page": 1}, timeout=15)
                if resp.status_code in (200, 422):  # 422 = parâmetros inválidos mas endpoint existe
                    print(f"[Guru] Endpoint de transações encontrado: {url}")
                    return base, path
                if resp.status_code == 401:
                    raise ValueError("[Guru] Token inválido (401). Verifique GURU_TOKEN no .env")
            except requests.exceptions.ConnectionError:
                continue
            except ValueError:
                raise

    raise RuntimeError(
        "[Guru] Não foi possível descobrir o endpoint da API. "
        "Verifique a documentação em https://docs.digitalmanager.guru/developers/"
    )


def fetch_paginated(base_url, path, extra_params=None):
    """
    Busca todos os registros de um endpoint com paginação (page/per_page).
    Retorna lista de itens brutos.
    """
    url = f"{base_url}{path}"
    all_items = []
    page = 1
    per_page = 100

    while True:
        params = {"page": page, "per_page": per_page}
        if extra_params:
            params.update(extra_params)

        print(f"[Guru] GET {path} — página {page} (total até agora: {len(all_items)})")

        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)

        if resp.status_code == 429:
            print("[Guru] Rate limit atingido. Aguardando 30s...")
            time.sleep(30)
            continue

        if resp.status_code == 401:
            raise ValueError("[Guru] Token expirado ou inválido.")

        if resp.status_code == 404:
            print(f"[Guru] Endpoint {path} retornou 404.")
            return []

        resp.raise_for_status()

        data = resp.json()

        # Suportar diferentes formatos de resposta
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("data")
                or data.get("items")
                or data.get("transactions")
                or data.get("subscriptions")
                or data.get("orders")
                or []
            )
        else:
            items = []

        if not items:
            print(f"[Guru] Página {page} vazia. Fim da paginação.")
            break

        all_items.extend(items)

        # Verificar se há próxima página
        if isinstance(data, dict):
            meta = data.get("meta", {}) or {}
            last_page = meta.get("last_page") or data.get("last_page")
            if last_page and page >= int(last_page):
                print(f"[Guru] Última página atingida ({page}/{last_page}).")
                break

        if len(items) < per_page:
            print(f"[Guru] Menos itens que per_page ({len(items)}<{per_page}). Fim.")
            break

        page += 1
        time.sleep(0.3)

    return all_items


def normalize_transaction(item):
    """
    Normaliza um item de transação/assinatura da Guru para o formato padrão.
    Suporta estruturas com contact, subscriber, buyer ou campos diretos.
    """
    contact = (
        item.get("contact")
        or item.get("subscriber")
        or item.get("buyer")
        or item.get("customer")
        or {}
    )

    # Email
    email = (
        contact.get("email")
        or item.get("email")
        or ""
    ).strip().lower()

    # Telefone
    phone_raw = (
        contact.get("phone_number")
        or contact.get("phone")
        or item.get("phone_number")
        or item.get("phone")
        or ""
    )
    phone_code = contact.get("phone_local_code") or ""
    phone = "".join(filter(str.isdigit, str(phone_code) + str(phone_raw)))

    # Nome
    nome = (
        contact.get("name")
        or item.get("name")
        or ""
    ).strip()

    # Data da compra
    dates = item.get("dates", {}) or {}
    data_compra = (
        dates.get("ordered_at")
        or dates.get("started_at")
        or dates.get("created_at")
        or item.get("ordered_at")
        or item.get("created_at")
        or item.get("date")
        or ""
    )
    # Padronizar para YYYY-MM-DD HH:MM:SS
    if data_compra and "T" in str(data_compra):
        data_compra = str(data_compra).replace("T", " ").split("+")[0].split("Z")[0]

    # Status — filtrar apenas pagamentos confirmados
    status = (
        item.get("status")
        or item.get("payment_status")
        or item.get("state")
        or ""
    ).lower()

    return {
        "email": email,
        "telefone": phone,
        "nome": nome,
        "data_compra": data_compra,
        "plataforma": "guru",
        "_status": status,
    }


def save_to_csv(records, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = ["email", "telefone", "nome", "data_compra", "plataforma"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    print(f"[Guru] {len(records)} registros salvos em {output_path}")


def main():
    print("=" * 50)
    print("GURU — Buscando histórico completo de transações")
    print("=" * 50)

    # Descobrir endpoint base
    base_url, transactions_path = discover_base_url()

    # Buscar transações
    print("\n[Guru] Buscando transações...")
    raw_transactions = fetch_paginated(base_url, transactions_path)

    # Tentar buscar assinaturas (endpoint diferente)
    print("\n[Guru] Buscando assinaturas...")
    subscription_paths = ["/subscriptions", "/signatures", "/recurring"]
    raw_subscriptions = []
    for sub_path in subscription_paths:
        sub_url = f"{base_url}{sub_path}"
        try:
            test = requests.get(sub_url, headers=HEADERS, params={"per_page": 1}, timeout=15)
            if test.status_code == 200:
                raw_subscriptions = fetch_paginated(base_url, sub_path)
                break
            elif test.status_code == 404:
                continue
        except Exception:
            continue

    # Normalizar e filtrar
    all_records = []
    skipped = 0
    for item in raw_transactions + raw_subscriptions:
        normalized = normalize_transaction(item)
        status = normalized.pop("_status", "")

        # Aceitar se status vazio (não sabemos) ou se é status aprovado
        if status and not any(s in status for s in PAID_STATUSES):
            skipped += 1
            continue

        if not normalized["email"] and not normalized["telefone"]:
            skipped += 1
            continue

        all_records.append(normalized)

    print(f"\n[Guru] Total bruto: {len(raw_transactions + raw_subscriptions)}")
    print(f"[Guru] Ignorados (sem contato ou não pagos): {skipped}")
    print(f"[Guru] Registros válidos: {len(all_records)}")

    save_to_csv(all_records, OUTPUT_FILE)
    print(f"\n[Guru] Concluído!")


if __name__ == "__main__":
    main()
