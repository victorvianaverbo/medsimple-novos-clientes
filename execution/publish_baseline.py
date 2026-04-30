"""
publish_baseline.py
Publica o .tmp/new_clients.csv como um GitHub Gist privado.

Na primeira execução: cria o Gist e salva o ID no .env.
Nas seguintes: atualiza o mesmo Gist.

Pré-requisito:
  GITHUB_GIST_TOKEN no .env — Personal Access Token com scope "gist"
  Criar em: https://github.com/settings/tokens/new → selecionar "gist"
"""

import io
import os
import re
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASELINE_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "new_clients.csv")
HOTMART_RAW   = os.path.join(os.path.dirname(__file__), "..", ".tmp", "hotmart_raw.csv")
GURU_RAW      = os.path.join(os.path.dirname(__file__), "..", ".tmp", "guru_raw.csv")
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")

GITHUB_API = "https://api.github.com"


def get_env(key):
    return os.getenv(key, "").strip()


def update_env_var(key, value):
    """Atualiza ou adiciona uma variável no .env."""
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = rf"^{key}=.*$"
    replacement = f"{key}={value}"

    if re.search(pattern, content, flags=re.MULTILINE):
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    else:
        content += f"\n{replacement}\n"

    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[.env] {key} atualizado.")


def main():
    print("=" * 55)
    print("PUBLISH BASELINE — GitHub Gist Privado")
    print("=" * 55)

    token = get_env("GITHUB_GIST_TOKEN")
    if not token:
        print("\n[ERRO] GITHUB_GIST_TOKEN não encontrado no .env")
        print("Crie em: https://github.com/settings/tokens/new")
        print("Scope necessário: 'gist'")
        return

    if not os.path.exists(BASELINE_FILE):
        print(f"\n[ERRO] Arquivo não encontrado: {BASELINE_FILE}")
        print("Execute primeiro: python execution/identify_new_clients.py")
        return

    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    print(f"Baseline carregado: {len(content.splitlines())-1} registros")

    # Gerar sales_by_year.csv a partir dos raw files
    sales_content = ""
    if os.path.exists(HOTMART_RAW) and os.path.exists(GURU_RAW):
        df_hm = pd.read_csv(HOTMART_RAW, dtype=str)
        df_hm["ano"] = pd.to_datetime(df_hm["data_compra"], errors="coerce").dt.year
        df_gu = pd.read_csv(GURU_RAW, dtype=str)
        df_gu["ano"] = pd.to_datetime(df_gu["data_compra"], errors="coerce").dt.year

        sales = pd.concat([
            df_hm.groupby("ano").size().rename("hotmart"),
            df_gu.groupby("ano").size().rename("guru"),
        ], axis=1).fillna(0).astype(int).reset_index()
        sales["total"] = sales["hotmart"] + sales["guru"]
        sales = sales[["ano", "total", "hotmart", "guru"]].sort_values("ano")

        buf = io.StringIO()
        sales.to_csv(buf, index=False)
        sales_content = buf.getvalue()
        print(f"Sales by year gerado: {len(sales)} anos")
    else:
        print("[!] Raw files não encontrados — sales_by_year.csv não será publicado")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    # Gerar sales_6anos.csv (impacto dos planos de 6 anos por ano)
    s6_content = ""
    if os.path.exists(GURU_RAW):
        df_gu2 = pd.read_csv(GURU_RAW, dtype=str)
        df_gu2["valor_liquido"] = pd.to_numeric(df_gu2.get("valor_liquido", 0), errors="coerce").fillna(0)
        df_gu2["ano"] = pd.to_datetime(df_gu2["data_compra"], errors="coerce").dt.year
        df_gu2["nome_oferta"] = df_gu2.get("nome_oferta", "").fillna("")
        mask_6a = df_gu2["nome_oferta"].str.lower().str.contains("6 ano", na=False)

        # Também incluir Hotmart (sem planos de 6 anos)
        if os.path.exists(HOTMART_RAW):
            df_hm2 = pd.read_csv(HOTMART_RAW, dtype=str)
            df_hm2["valor_liquido"] = pd.to_numeric(df_hm2.get("valor_liquido", 0), errors="coerce").fillna(0)
            df_hm2["ano"] = pd.to_datetime(df_hm2["data_compra"], errors="coerce").dt.year
        else:
            df_hm2 = pd.DataFrame(columns=["ano", "valor_liquido"])

        rows_6a = []
        all_anos = sorted(set(df_gu2["ano"].dropna().unique()) | set(df_hm2["ano"].dropna().unique()))
        for ano in all_anos:
            g6 = df_gu2[(df_gu2["ano"] == ano) & mask_6a]
            g_gu = df_gu2[df_gu2["ano"] == ano]
            g_hm = df_hm2[df_hm2["ano"] == ano] if not df_hm2.empty else pd.DataFrame()
            total_trans = len(g_gu) + len(g_hm)
            total_rec = g_gu["valor_liquido"].sum() + (g_hm["valor_liquido"].sum() if not g_hm.empty else 0)
            rows_6a.append({
                "ano": int(ano),
                "trans_6anos": len(g6),
                "receita_6anos": round(g6["valor_liquido"].sum(), 2),
                "trans_outros": total_trans - len(g6),
                "receita_outros": round(total_rec - g6["valor_liquido"].sum(), 2),
                "trans_total": total_trans,
                "receita_total": round(total_rec, 2),
            })

        df_6a = pd.DataFrame(rows_6a)
        buf6 = io.StringIO()
        df_6a.to_csv(buf6, index=False)
        s6_content = buf6.getvalue()
        print(f"Sales 6 anos gerado: {len(df_6a)} anos | {mask_6a.sum()} transações de 6 anos")

    # Gerar sales_by_product.csv (vendas por produto/oferta por ano)
    sp_content = ""
    if os.path.exists(HOTMART_RAW) and os.path.exists(GURU_RAW):
        df_hm3 = pd.read_csv(HOTMART_RAW, dtype=str)
        df_hm3["valor_liquido"] = pd.to_numeric(df_hm3.get("valor_liquido", 0), errors="coerce").fillna(0)
        df_hm3["ano"] = pd.to_datetime(df_hm3["data_compra"], errors="coerce").dt.year
        df_hm3["produto"] = df_hm3["nome_produto"].fillna("Sem nome")
        df_hm3["plataforma"] = "hotmart"

        df_gu3 = pd.read_csv(GURU_RAW, dtype=str)
        df_gu3["valor_liquido"] = pd.to_numeric(df_gu3.get("valor_liquido", 0), errors="coerce").fillna(0)
        df_gu3["ano"] = pd.to_datetime(df_gu3["data_compra"], errors="coerce").dt.year
        df_gu3["produto"] = df_gu3["nome_oferta"].fillna(df_gu3["nome_produto"]).fillna("Sem nome")
        df_gu3["plataforma"] = "guru"

        df_all = pd.concat([
            df_hm3[["produto", "plataforma", "ano", "valor_liquido"]],
            df_gu3[["produto", "plataforma", "ano", "valor_liquido"]],
        ], ignore_index=True)

        sp = df_all.groupby(["produto", "plataforma", "ano"]).agg(
            vendas=("valor_liquido", "count"),
            receita=("valor_liquido", "sum"),
        ).reset_index()
        sp["receita"] = sp["receita"].round(2)
        sp = sp.sort_values(["ano", "vendas"], ascending=[True, False])

        buf_sp = io.StringIO()
        sp.to_csv(buf_sp, index=False)
        sp_content = buf_sp.getvalue()
        print(f"Sales by product gerado: {len(sp)} linhas | {sp['produto'].nunique()} produtos distintos")

    # Gerar renewals_2026.csv (taxa de renovação mensal — só 2026)
    rn_content = ""
    if os.path.exists(HOTMART_RAW) and os.path.exists(GURU_RAW):
        df_hm4 = pd.read_csv(HOTMART_RAW, dtype=str)
        df_hm4["plataforma"] = "hotmart"
        df_gu4 = pd.read_csv(GURU_RAW, dtype=str)
        df_gu4["plataforma"] = "guru"
        df_tx = pd.concat([
            df_hm4[["email", "telefone", "data_compra", "plataforma"]],
            df_gu4[["email", "telefone", "data_compra", "plataforma"]],
        ], ignore_index=True)
        df_tx["email"] = df_tx["email"].fillna("").str.strip().str.lower()
        df_tx["telefone"] = df_tx["telefone"].fillna("").astype(str).str.replace(r"\D", "", regex=True)
        df_tx["data_compra"] = pd.to_datetime(df_tx["data_compra"], errors="coerce")
        df_tx = df_tx.dropna(subset=["data_compra"]).reset_index(drop=True)

        # Union-Find por email OU telefone
        parent = {i: i for i in df_tx.index}
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        def union(a, b):
            parent[find(a)] = find(b)

        emap, pmap = {}, {}
        for i, row in df_tx.iterrows():
            e, p = row["email"], row["telefone"]
            if e:
                if e in emap: union(i, emap[e])
                emap.setdefault(e, i)
            if p and len(p) >= 8:
                if p in pmap: union(i, pmap[p])
                pmap.setdefault(p, i)
        df_tx["client_id"] = [find(i) for i in df_tx.index]

        # Para cada transação, achar a primeira data daquele cliente
        primeira = df_tx.groupby("client_id")["data_compra"].min().rename("primeira_compra")
        df_tx = df_tx.merge(primeira, on="client_id")
        df_tx["is_recompra"] = df_tx["data_compra"] > df_tx["primeira_compra"]

        # Filtrar 2026 e agregar por mês
        df_2026 = df_tx[df_tx["data_compra"].dt.year == 2026].copy()
        df_2026["mes"] = df_2026["data_compra"].dt.month
        rn_rows = []
        for mes in sorted(df_2026["mes"].unique()):
            grp = df_2026[df_2026["mes"] == mes]
            total = len(grp)
            recompras = int(grp["is_recompra"].sum())
            novos = total - recompras
            taxa = round(recompras / total * 100, 1) if total else 0
            rn_rows.append({
                "mes": int(mes),
                "total_trans": total,
                "novos": novos,
                "recompras": recompras,
                "taxa_renovacao_pct": taxa,
                "hotmart_recompras": int(grp[grp["is_recompra"] & (grp["plataforma"] == "hotmart")].shape[0]),
                "guru_recompras": int(grp[grp["is_recompra"] & (grp["plataforma"] == "guru")].shape[0]),
            })
        df_rn = pd.DataFrame(rn_rows)
        rn_local = os.path.join(os.path.dirname(__file__), "..", ".tmp", "renewals_2026.csv")
        df_rn.to_csv(rn_local, index=False)
        buf_rn = io.StringIO()
        df_rn.to_csv(buf_rn, index=False)
        rn_content = buf_rn.getvalue()
        print(f"Renewals 2026 gerado: {len(df_rn)} meses | taxa média {df_rn['taxa_renovacao_pct'].mean():.1f}%")
        print(df_rn.to_string(index=False))

    files_payload = {"new_clients.csv": {"content": content}}
    if sales_content:
        files_payload["sales_by_year.csv"] = {"content": sales_content}
    if s6_content:
        files_payload["sales_6anos.csv"] = {"content": s6_content}
    if sp_content:
        files_payload["sales_by_product.csv"] = {"content": sp_content}
    if rn_content:
        files_payload["renewals_2026.csv"] = {"content": rn_content}

    payload = {
        "description": "Medsimple — Baseline novos clientes (gerado automaticamente)",
        "public": False,
        "files": files_payload,
    }

    gist_id = get_env("GITHUB_GIST_ID")

    if gist_id:
        print(f"\nAtualizando Gist existente: {gist_id}...")
        resp = requests.patch(f"{GITHUB_API}/gists/{gist_id}", json=payload, headers=headers, timeout=30)
        action = "atualizado"
    else:
        print("\nCriando novo Gist privado...")
        resp = requests.post(f"{GITHUB_API}/gists", json=payload, headers=headers, timeout=30)
        action = "criado"

    if resp.status_code in (200, 201):
        data = resp.json()
        gist_id = data["id"]
        gist_url = data["html_url"]
        update_env_var("GITHUB_GIST_ID", gist_id)
        print(f"\n[OK] Gist {action} com sucesso!")
        print(f"ID: {gist_id}")
        print(f"URL: {gist_url}")
        print("\nAdicione ao Streamlit Cloud secrets:")
        print(f'  GITHUB_GIST_TOKEN = "{token}"')
        print(f'  GITHUB_GIST_ID = "{gist_id}"')
    else:
        print(f"\n[ERRO] Status {resp.status_code}: {resp.text[:300]}")


if __name__ == "__main__":
    main()
