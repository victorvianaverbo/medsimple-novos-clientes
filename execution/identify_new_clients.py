"""
identify_new_clients.py
Cruza os dados de Hotmart e Guru para identificar novos clientes.

"Novo cliente" = pessoa cuja PRIMEIRA compra (em qualquer plataforma) ocorreu no período analisado.
Cruzamento por email (prioridade) e/ou telefone.

Input:
  .tmp/hotmart_raw.csv
  .tmp/guru_raw.csv

Output:
  .tmp/new_clients.csv  — um registro por cliente único, com data da primeira compra
"""

import os
import csv
import pandas as pd
from datetime import datetime

HOTMART_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "hotmart_raw.csv")
GURU_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "guru_raw.csv")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "new_clients.csv")


def normalize_email(email):
    if pd.isna(email) or not str(email).strip():
        return ""
    return str(email).strip().lower()


def normalize_phone(phone):
    if pd.isna(phone) or not str(phone).strip():
        return ""
    digits = "".join(filter(str.isdigit, str(phone)))
    # Remover DDI +55 se presente
    if digits.startswith("55") and len(digits) >= 12:
        digits = digits[2:]
    # Manter apenas telefones com 10-11 dígitos (Brasil)
    if len(digits) < 8 or len(digits) > 13:
        return ""
    return digits


def load_platform(filepath, platform_name):
    """Carrega CSV de uma plataforma e normaliza campos."""
    if not os.path.exists(filepath):
        print(f"[!] Arquivo não encontrado: {filepath}")
        print(f"    Execute fetch_{platform_name}.py primeiro.")
        return pd.DataFrame()

    df = pd.read_csv(filepath, dtype=str)
    df["plataforma"] = platform_name
    df["email"] = df["email"].apply(normalize_email)
    df["telefone"] = df["telefone"].apply(normalize_phone)
    df["data_compra"] = pd.to_datetime(df["data_compra"], errors="coerce", utc=True)
    df["valor_liquido"] = pd.to_numeric(df.get("valor_liquido", 0), errors="coerce").fillna(0)
    if "nome_produto" not in df.columns:
        df["nome_produto"] = ""
    else:
        df["nome_produto"] = df["nome_produto"].fillna("")
    df = df.dropna(subset=["data_compra"])
    print(f"[{platform_name.upper()}] {len(df)} registros carregados.")
    return df


def merge_by_email_and_phone(df):
    """
    Agrupa registros pelo mesmo cliente (email OU telefone).
    Usa Union-Find para conectar registros que compartilham email ou telefone.
    Retorna df com coluna 'client_id' identificando cada cliente único.
    """
    n = len(df)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    # Índice email → lista de posições
    email_index = {}
    phone_index = {}

    for i, row in df.iterrows():
        email = row["email"]
        phone = row["telefone"]

        if email:
            if email in email_index:
                union(i, email_index[email][0])
            else:
                email_index[email] = []
            email_index[email].append(i)

        if phone and len(phone) >= 8:
            if phone in phone_index:
                union(i, phone_index[phone][0])
            else:
                phone_index[phone] = []
            phone_index[phone].append(i)

    df = df.copy()
    df["client_id"] = [find(i) for i in df.index]
    return df


def identify_new_clients(df):
    """
    Para cada cliente único, encontra a data da primeira compra e agrega métricas.
    Retorna um df com um registro por cliente.
    """
    # Primeira compra por cliente
    first_purchase = df.sort_values("data_compra").groupby("client_id").first().reset_index()

    # Agregações: LTV (soma), nº de compras, data da última compra
    agg = df.groupby("client_id").agg(
        ltv=("valor_liquido", "sum"),
        n_compras=("data_compra", "count"),
        data_ultima_compra=("data_compra", "max"),
    ).reset_index()

    result = first_purchase.merge(agg, on="client_id", how="left")

    # Formatar datas e valores
    result["ano"] = result["data_compra"].dt.year
    result["mes"] = result["data_compra"].dt.month
    result["data_primeira_compra"] = result["data_compra"].dt.strftime("%Y-%m-%d %H:%M:%S")
    result["data_ultima_compra"] = result["data_ultima_compra"].dt.strftime("%Y-%m-%d %H:%M:%S")
    result["ltv"] = result["ltv"].fillna(0).round(2)
    result["n_compras"] = result["n_compras"].fillna(1).astype(int)

    return result[[
        "email", "telefone", "nome", "data_primeira_compra", "plataforma",
        "ano", "mes", "ltv", "n_compras", "data_ultima_compra", "nome_produto",
    ]].rename(columns={"plataforma": "plataforma_primeira_compra"})


def print_summary(df):
    """Exibe resumo no terminal."""
    print("\n" + "=" * 50)
    print("RESUMO — NOVOS CLIENTES POR ANO")
    print("=" * 50)

    for ano in sorted(df["ano"].unique()):
        total = len(df[df["ano"] == ano])
        hotmart = len(df[(df["ano"] == ano) & (df["plataforma_primeira_compra"] == "hotmart")])
        guru = len(df[(df["ano"] == ano) & (df["plataforma_primeira_compra"] == "guru")])
        print(f"\n{int(ano)}:")
        print(f"  Total novos clientes : {total}")
        print(f"  Via Hotmart          : {hotmart}")
        print(f"  Via Guru             : {guru}")

    print(f"\nTotal histórico de clientes únicos: {len(df)}")


def main():
    print("=" * 50)
    print("IDENTIFICAÇÃO DE NOVOS CLIENTES")
    print("=" * 50)

    # Carregar dados
    df_hotmart = load_platform(HOTMART_FILE, "hotmart")
    df_guru = load_platform(GURU_FILE, "guru")

    if df_hotmart.empty and df_guru.empty:
        print("[ERRO] Nenhum dado encontrado. Execute os scripts de fetch primeiro.")
        return

    # Combinar
    df_all = pd.concat([df_hotmart, df_guru], ignore_index=True)
    print(f"\nTotal de registros combinados: {len(df_all)}")

    # Cruzar por email/telefone
    print("\nCruzando clientes por email e telefone...")
    df_merged = merge_by_email_and_phone(df_all)
    print(f"Clientes únicos identificados: {df_merged['client_id'].nunique()}")

    # Identificar primeira compra de cada cliente
    result = identify_new_clients(df_merged)

    # Salvar
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\nSalvo em: {OUTPUT_FILE}")

    # Resumo
    print_summary(result)


if __name__ == "__main__":
    main()
