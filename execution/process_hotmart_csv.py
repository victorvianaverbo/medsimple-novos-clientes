"""
process_hotmart_csv.py
Processa os exports CSV da Hotmart e salva em .tmp/hotmart_raw.csv

Filtros aplicados:
  - Status in ["Aprovado", "Completo"]
  - Produto: já vem pré-filtrado nos exports (Plataforma MEDsimple apenas)

Formato dos CSVs:
  - Delimitador: ponto e vírgula (;)
  - Encoding: utf-8-sig
  - Data de Confirmação: DD/MM/YYYY HH:MM:SS
"""

import os
import csv
from datetime import datetime

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "hotmart_raw.csv")

CSV_FILES = [
    r"F:\Downloads\sales_history_20260320154544_FB2456C83882739997474803165.csv",
    r"F:\Downloads\sales_history_20260320154431_473E7C3A5715212713349273767.csv",
    r"F:\Downloads\sales_history_20260320132919_41DAFA167027452998184058871.csv",
    r"F:\Downloads\sales_history_20260320151431_8219BF837955421160596342750.csv",
    r"F:\Downloads\sales_history_20260320132640_8584E6A83289272021253433931.csv",
    r"F:\Downloads\sales_history_20260320154900_5DCEB2B413074293895924279648.csv",
]

STATUS_VALIDOS = {"aprovado", "completo"}


def normalize_phone(ddd, number):
    digits = "".join(filter(str.isdigit, str(ddd or "") + str(number or "")))
    # Remover DDI +55 se presente
    if digits.startswith("55") and len(digits) >= 12:
        digits = digits[2:]
    return digits


def parse_date(value):
    if not value or str(value).strip() == "":
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value).strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def find_col(headers, *candidates):
    """
    Encontra o nome original de uma coluna pelos candidatos (case-insensitive).
    Prioriza match exato antes de match parcial.
    """
    h_lower = [h.lower().strip() for h in headers]
    for candidate in candidates:
        cand = candidate.lower()
        for i, h in enumerate(h_lower):
            if h == cand:
                return headers[i]
        for i, h in enumerate(h_lower):
            if cand in h:
                return headers[i]
    return None


def process_file(filepath):
    print(f"\n[Hotmart] Processando: {os.path.basename(filepath)}")

    records = []
    skipped_status = 0
    skipped_date = 0

    with open(filepath, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        headers = reader.fieldnames or []

        # Encontrar colunas (tolera encoding corrompido nos nomes)
        col_status = find_col(headers, "status")
        col_email = find_col(headers, "email")
        col_nome = find_col(headers, "nome")
        col_ddd = find_col(headers, "ddd")
        col_tel = find_col(headers, "telefone")
        col_data = find_col(headers, "confirma")  # "Data de Confirmação"
        col_produto = find_col(headers, "nome do produto", "produto")

        print(f"  status={col_status} | email={col_email} | nome={col_nome} | tel={col_tel} | data={col_data} | produto={col_produto}")

        for row in reader:
            status = str(row.get(col_status, "") or "").strip().lower()
            if status not in STATUS_VALIDOS:
                skipped_status += 1
                continue

            data = parse_date(row.get(col_data, ""))
            if not data:
                skipped_date += 1
                continue

            email = str(row.get(col_email, "") or "").strip().lower()
            nome = str(row.get(col_nome, "") or "").strip()
            ddd = str(row.get(col_ddd, "") or "").strip()
            tel = str(row.get(col_tel, "") or "").strip()
            telefone = normalize_phone(ddd, tel)

            if not email and not telefone:
                continue

            records.append({
                "email": email,
                "telefone": telefone,
                "nome": nome,
                "data_compra": data,
                "plataforma": "hotmart",
            })

    print(f"  Válidos: {len(records)} | Ignorados status: {skipped_status} | Sem data: {skipped_date}")
    return records


def main():
    print("=" * 55)
    print("HOTMART — Processando CSVs históricos")
    print("=" * 55)

    all_records = []
    for f in CSV_FILES:
        if not os.path.exists(f):
            print(f"[!] Arquivo não encontrado: {f}")
            continue
        all_records.extend(process_file(f))

    # Remover duplicatas exatas
    seen = set()
    unique = []
    for r in all_records:
        key = (r["email"], r["telefone"], r["data_compra"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "telefone", "nome", "data_compra", "plataforma"])
        writer.writeheader()
        writer.writerows(unique)

    print(f"\n[Hotmart] Total bruto: {len(all_records)} | Únicos: {len(unique)}")
    print(f"[Hotmart] Salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
