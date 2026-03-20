"""
app.py — Dashboard de Novos Clientes (Medsimple)
Versão produção: busca dados diretamente das APIs Hotmart e Guru.

Rodar localmente:   streamlit run app.py
Deploy:             Streamlit Community Cloud (share.streamlit.io)
"""

import os
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone, date

# ── Configuração ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Novos Clientes — Medsimple",
    page_icon="📊",
    layout="wide",
)

MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

PAID_STATUSES = {"paid", "approved", "complete", "active", "confirmed"}


# ── Credenciais (Streamlit Cloud secrets ou .env local) ─────────────────────
def get_secret(key: str) -> str:
    try:
        return st.secrets[key]
    except Exception:
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv(key, "")


# ── Hotmart ─────────────────────────────────────────────────────────────────
def hotmart_get_token() -> str:
    basic = get_secret("HOTMART_BASIC")
    # Garante que o prefixo 'Basic ' está presente
    if not basic.startswith("Basic "):
        basic = f"Basic {basic}"
    resp = requests.post(
        "https://api-sec-vlc.hotmart.com/security/oauth/token",
        headers={"Authorization": basic, "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def ms_to_dt(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except Exception:
        return None


def hotmart_fetch_all(progress_cb=None) -> list[dict]:
    token = hotmart_get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://developers.hotmart.com/payments/api/v1/sales/history"
    records, page_token, page = [], None, 1

    while True:
        # transaction_status deve ser enviado como parâmetros repetidos, não string única
        params = [("max_results", 500), ("transaction_status", "APPROVED"), ("transaction_status", "COMPLETE")]
        if page_token:
            params.append(("page_token", page_token))

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(30)
            continue
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            buyer = item.get("buyer") or {}
            purchase = item.get("purchase") or {}
            phone = "".join(filter(str.isdigit, str(buyer.get("phone") or buyer.get("document") or "")))
            if len(phone) == 11 and not phone.startswith("0"):
                phone = ""  # provavelmente CPF
            records.append({
                "email": (buyer.get("email") or "").strip().lower(),
                "telefone": phone,
                "nome": (buyer.get("name") or "").strip(),
                "data_compra": ms_to_dt(purchase.get("approved_date") or purchase.get("order_date")),
                "plataforma": "hotmart",
            })

        pi = data.get("page_info") or {}
        next_token = pi.get("next_page_token")
        if progress_cb:
            progress_cb(f"Hotmart: {len(records)} vendas (página {page})...")
        if not next_token or not data.get("items"):
            break
        page_token, page = next_token, page + 1
        time.sleep(0.4)

    return records


# ── Guru ────────────────────────────────────────────────────────────────────
def guru_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_secret('GURU_TOKEN')}",
        "Accept": "application/json",
    }


def guru_discover_endpoint() -> tuple[str, str]:
    """Descobre a URL base + path correto da API Guru."""
    candidates = [
        ("https://digitalmanager.guru/api/v2", "/transactions"),
        ("https://digitalmanager.guru/api/v1", "/transactions"),
        ("https://digitalmanager.guru/api/v2", "/sales"),
        ("https://digitalmanager.guru/api/v1", "/sales"),
        ("https://api.digitalmanager.guru/v2", "/transactions"),
        ("https://api.digitalmanager.guru/v1", "/transactions"),
    ]
    for base, path in candidates:
        try:
            r = requests.get(f"{base}{path}", headers=guru_headers(), params={"per_page": 1}, timeout=15)
            if r.status_code in (200, 422):
                return base, path
            if r.status_code == 401:
                raise ValueError("GURU_TOKEN inválido (401)")
        except (requests.ConnectionError, requests.Timeout):
            continue
    raise RuntimeError("Não foi possível descobrir o endpoint da Guru API.")


def guru_normalize(item: dict) -> dict | None:
    contact = item.get("contact") or item.get("subscriber") or item.get("buyer") or item.get("customer") or {}
    email = (contact.get("email") or item.get("email") or "").strip().lower()
    phone_code = str(contact.get("phone_local_code") or "")
    phone_raw = str(contact.get("phone_number") or contact.get("phone") or item.get("phone_number") or "")
    phone = "".join(filter(str.isdigit, phone_code + phone_raw))
    if not email and not phone:
        return None
    dates = item.get("dates") or {}
    raw_date = dates.get("ordered_at") or dates.get("started_at") or dates.get("created_at") or item.get("created_at") or ""
    try:
        dt = pd.to_datetime(raw_date, utc=True)
    except Exception:
        dt = None
    status = (item.get("status") or item.get("payment_status") or "").lower()
    if status and not any(s in status for s in PAID_STATUSES):
        return None
    return {
        "email": email,
        "telefone": phone,
        "nome": (contact.get("name") or item.get("name") or "").strip(),
        "data_compra": dt,
        "plataforma": "guru",
    }


def guru_fetch_path(base: str, path: str, progress_cb=None) -> list[dict]:
    records, page = [], 1
    while True:
        r = requests.get(
            f"{base}{path}", headers=guru_headers(),
            params={"page": page, "per_page": 100}, timeout=60,
        )
        if r.status_code == 429:
            time.sleep(30)
            continue
        if r.status_code in (404, 405):
            break
        r.raise_for_status()
        data = r.json()
        items = (data if isinstance(data, list)
                 else data.get("data") or data.get("items") or data.get("transactions") or data.get("subscriptions") or [])
        if not items:
            break
        for item in items:
            norm = guru_normalize(item)
            if norm and norm["data_compra"] is not None:
                records.append(norm)
        if progress_cb:
            progress_cb(f"Guru{path}: {len(records)} registros (página {page})...")
        meta = (data if isinstance(data, dict) else {}).get("meta") or {}
        if meta.get("last_page") and page >= int(meta["last_page"]):
            break
        if len(items) < 100:
            break
        page += 1
        time.sleep(0.3)
    return records


def guru_fetch_all(progress_cb=None) -> list[dict]:
    base, tx_path = guru_discover_endpoint()
    records = guru_fetch_path(base, tx_path, progress_cb)
    for sub_path in ["/subscriptions", "/signatures"]:
        try:
            r = requests.get(f"{base}{sub_path}", headers=guru_headers(), params={"per_page": 1}, timeout=10)
            if r.status_code == 200:
                records += guru_fetch_path(base, sub_path, progress_cb)
                break
        except Exception:
            continue
    return records


# ── Cruzamento Union-Find ───────────────────────────────────────────────────
def find_unique_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa registros que compartilham email OU telefone (Union-Find).
    Retorna df com coluna client_id.
    """
    idx = list(df.index)
    parent = {i: i for i in idx}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    email_map: dict[str, int] = {}
    phone_map: dict[str, int] = {}

    for i, row in df.iterrows():
        e, p = row["email"], row["telefone"]
        if e:
            if e in email_map:
                union(i, email_map[e])
            email_map.setdefault(e, i)
        if p and len(p) >= 8:
            if p in phone_map:
                union(i, phone_map[p])
            phone_map.setdefault(p, i)

    df = df.copy()
    df["client_id"] = [find(i) for i in df.index]
    first = df.sort_values("data_compra").groupby("client_id").first().reset_index()
    first["ano"] = first["data_compra"].dt.year.astype("Int64")
    first["mes"] = first["data_compra"].dt.month.astype("Int64")
    first["data_primeira_compra"] = first["data_compra"]
    return first.rename(columns={"plataforma": "plataforma_primeira_compra"})


# ── Fetch completo com cache ────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_data() -> pd.DataFrame:
    status_box = st.empty()

    def progress(msg):
        status_box.info(f"⏳ {msg}")

    progress("Conectando às APIs...")
    try:
        hotmart_records = hotmart_fetch_all(progress)
    except Exception as e:
        st.error(f"Erro ao buscar Hotmart: {e}")
        hotmart_records = []

    try:
        guru_records = guru_fetch_all(progress)
    except Exception as e:
        st.error(f"Erro ao buscar Guru: {e}")
        guru_records = []

    status_box.empty()

    if not hotmart_records and not guru_records:
        return pd.DataFrame()

    df = pd.DataFrame(hotmart_records + guru_records)
    df = df.dropna(subset=["data_compra"])
    df["email"] = df["email"].fillna("")
    df["telefone"] = df["telefone"].fillna("")

    return find_unique_clients(df)


# ── Filtros ─────────────────────────────────────────────────────────────────
def apply_filters(df, ano, mes, plataforma, periodo):
    if df.empty:
        return df
    if periodo:
        s, e = periodo
        df = df[(df["data_primeira_compra"].dt.date >= s) & (df["data_primeira_compra"].dt.date <= e)]
    else:
        if ano != "Todos":
            df = df[df["ano"] == int(ano)]
        if mes != "Todos":
            mn = next(k for k, v in MESES.items() if v == mes)
            df = df[df["mes"] == mn]
    if plataforma != "Todas":
        df = df[df["plataforma_primeira_compra"] == plataforma.lower()]
    return df


# ── UI ──────────────────────────────────────────────────────────────────────
def render_kpis(df, df_full):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Novos no período", f"{len(df):,}".replace(",", "."))
    c2.metric("Via Hotmart", f"{len(df[df.plataforma_primeira_compra=='hotmart']):,}".replace(",", "."))
    c3.metric("Via Guru", f"{len(df[df.plataforma_primeira_compra=='guru']):,}".replace(",", "."))
    c4.metric("Total histórico", f"{len(df_full):,}".replace(",", "."))


def render_charts(df):
    if df.empty:
        st.info("Nenhum dado para os filtros selecionados.")
        return
    st.divider()
    df2 = df.copy()
    df2["mes_ano"] = df2["data_primeira_compra"].dt.to_period("M").astype(str)
    pivot = (
        df2.groupby(["mes_ano", "plataforma_primeira_compra"])
        .size().reset_index(name="n")
        .pivot(index="mes_ano", columns="plataforma_primeira_compra", values="n")
        .fillna(0).sort_index()
    )
    totais = df2.groupby("mes_ano").size().reset_index(name="Novos Clientes").set_index("mes_ano").sort_index()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Evolução mensal")
        st.line_chart(totais, use_container_width=True)
    with col2:
        st.subheader("Hotmart vs Guru")
        st.bar_chart(pivot, use_container_width=True)


def render_table(df):
    st.divider()
    st.subheader(f"Clientes ({len(df):,} registros)")
    q = st.text_input("Buscar por nome, email ou telefone", placeholder="ex: joao@email.com")
    if q:
        mask = (
            df["email"].str.contains(q, case=False, na=False) |
            df["nome"].str.contains(q, case=False, na=False) |
            df["telefone"].str.contains(q, na=False)
        )
        df = df[mask]
    cols = ["nome", "email", "telefone", "data_primeira_compra", "plataforma_primeira_compra", "ano", "mes"]
    cols = [c for c in cols if c in df.columns]
    st.dataframe(
        df[cols].sort_values("data_primeira_compra", ascending=False),
        use_container_width=True, hide_index=True,
        column_config={
            "nome": "Nome",
            "email": "Email",
            "telefone": "Telefone",
            "data_primeira_compra": st.column_config.DatetimeColumn("Primeira Compra", format="DD/MM/YYYY"),
            "plataforma_primeira_compra": "Plataforma",
            "ano": st.column_config.NumberColumn("Ano"),
            "mes": st.column_config.NumberColumn("Mês"),
        },
    )
    csv = df[cols].to_csv(index=False).encode("utf-8")
    st.download_button("Baixar CSV", csv, "novos_clientes.csv", "text/csv")


def main():
    st.title("📊 Novos Clientes — Medsimple")
    st.caption("Primeira compra em qualquer plataforma (Hotmart + Guru cruzados por email/telefone)")

    # Sidebar
    with st.sidebar:
        st.header("Filtros")
        df_full = load_data()

        if df_full.empty:
            st.error("Sem dados. Verifique as credenciais das APIs.")
            return

        anos = ["Todos"] + sorted([str(a) for a in df_full["ano"].dropna().unique()], reverse=True)
        filtro_ano = st.selectbox("Ano", anos)
        filtro_mes = st.selectbox("Mês", ["Todos"] + list(MESES.values()))
        st.divider()
        usar_periodo = st.checkbox("Período personalizado")
        filtro_periodo = None
        if usar_periodo:
            dmin = df_full["data_primeira_compra"].min().date()
            dmax = df_full["data_primeira_compra"].max().date()
            c1, c2 = st.columns(2)
            s = c1.date_input("De", value=date(2024, 1, 1), min_value=dmin, max_value=dmax)
            e = c2.date_input("Até", value=dmax, min_value=dmin, max_value=dmax)
            filtro_periodo = (s, e)
        st.divider()
        filtro_plataforma = st.radio("Plataforma", ["Todas", "Hotmart", "Guru"])
        st.divider()
        if st.button("🔄 Recarregar dados das APIs", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    df_filtered = apply_filters(df_full, filtro_ano, filtro_mes, filtro_plataforma, filtro_periodo)
    render_kpis(df_filtered, df_full)
    render_charts(df_filtered)
    render_table(df_filtered)


main()
