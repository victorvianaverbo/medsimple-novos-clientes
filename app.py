"""
app.py — Dashboard de Novos Clientes (Medsimple)
Versão produção: busca dados diretamente das APIs Hotmart e Guru.

Rodar localmente:   streamlit run app.py
Deploy:             Streamlit Community Cloud (share.streamlit.io)
"""

import io
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
GURU_BASE = "https://digitalmanager.guru/api/v2"
GURU_TX_PATH = "/transactions"
GURU_WINDOW_DAYS = 179  # limite da API: max 180 dias por request
GURU_START_DATE = date(2020, 1, 1)  # busca desde o início


def guru_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_secret('GURU_TOKEN')}",
        "Accept": "application/json",
    }


def guru_normalize(item: dict) -> dict | None:
    contact = item.get("contact") or {}
    email = (contact.get("email") or "").strip().lower()

    # Telefone: concatenar DDI + número e remover DDI 55 do Brasil
    phone_code = "".join(filter(str.isdigit, str(contact.get("phone_local_code") or "")))
    phone_raw = "".join(filter(str.isdigit, str(contact.get("phone_number") or "")))
    phone = phone_code + phone_raw
    if phone.startswith("55") and len(phone) >= 12:
        phone = phone[2:]

    if not email and not phone:
        return None

    # ordered_at é Unix timestamp (segundos)
    dates = item.get("dates") or {}
    ts = dates.get("ordered_at") or dates.get("confirmed_at") or dates.get("created_at")
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc) if ts else None
    except Exception:
        dt = None

    status = (item.get("status") or "").lower()
    if status and not any(s in status for s in PAID_STATUSES):
        return None

    # Filtrar apenas produto "Plataforma MEDsimple"
    product_name = ((item.get("product") or {}).get("name") or "").lower()
    if product_name and "plataforma medsimple" not in product_name:
        return None

    return {
        "email": email,
        "telefone": phone,
        "nome": (contact.get("name") or "").strip(),
        "data_compra": dt,
        "plataforma": "guru",
    }


def guru_fetch_window(ini: date, end: date, progress_cb=None) -> list[dict]:
    """Busca todas as transações numa janela de datas usando cursor."""
    records = []
    cursor = None
    page = 1

    while True:
        params = {
            "ordered_at_ini": ini.strftime("%Y-%m-%d"),
            "ordered_at_end": end.strftime("%Y-%m-%d"),
            "per_page": 100,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            f"{GURU_BASE}{GURU_TX_PATH}",
            headers=guru_headers(),
            params=params,
            timeout=60,
        )
        if r.status_code == 429:
            time.sleep(30)
            continue
        r.raise_for_status()
        data = r.json()

        for item in data.get("data") or []:
            norm = guru_normalize(item)
            if norm and norm["data_compra"] is not None:
                records.append(norm)

        if progress_cb:
            progress_cb(f"Guru {ini} → {end}: {len(records)} registros (pág {page})...")

        if not data.get("has_more_pages"):
            break
        cursor = data.get("next_cursor")
        if not cursor:
            break
        page += 1
        time.sleep(0.3)

    return records


def guru_fetch_all(progress_cb=None) -> list[dict]:
    """
    Busca todo o histórico dividindo em janelas de GURU_WINDOW_DAYS dias.
    A API exige filtro de data e limita a 180 dias por request.
    """
    from datetime import timedelta

    records = []
    window_start = GURU_START_DATE
    today = date.today()

    while window_start <= today:
        window_end = min(window_start + timedelta(days=GURU_WINDOW_DAYS), today)
        try:
            chunk = guru_fetch_window(window_start, window_end, progress_cb)
            records.extend(chunk)
        except Exception as e:
            if progress_cb:
                progress_cb(f"Guru: erro na janela {window_start}→{window_end}: {e}")
        window_start = window_end + timedelta(days=1)

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


# ── Baseline (GitHub Gist) + API incremental ────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def load_baseline_from_gist() -> pd.DataFrame:
    """Carrega o baseline pré-processado do GitHub Gist privado."""
    token = get_secret("GITHUB_GIST_TOKEN")
    gist_id = get_secret("GITHUB_GIST_ID")
    if not token or not gist_id:
        return pd.DataFrame()
    r = requests.get(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    r.raise_for_status()
    file_info = r.json()["files"]["new_clients.csv"]
    # GitHub trunca arquivos > 1 MB no campo "content" — usar raw_url para arquivos grandes
    if file_info.get("truncated"):
        raw_url = file_info["raw_url"]
        r2 = requests.get(raw_url, headers={"Authorization": f"token {token}"}, timeout=60)
        r2.raise_for_status()
        content = r2.text
    else:
        content = file_info["content"]
    df = pd.read_csv(io.StringIO(content), dtype=str)
    df["data_primeira_compra"] = pd.to_datetime(df["data_primeira_compra"], errors="coerce", utc=True)
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_sales_from_gist() -> pd.DataFrame:
    """Carrega sales_by_year.csv do Gist (totais de transações por ano)."""
    token = get_secret("GITHUB_GIST_TOKEN")
    gist_id = get_secret("GITHUB_GIST_ID")
    if not token or not gist_id:
        return pd.DataFrame()
    r = requests.get(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    r.raise_for_status()
    file_info = r.json()["files"].get("sales_by_year.csv")
    if not file_info:
        return pd.DataFrame()
    if file_info.get("truncated") or not file_info.get("content"):
        r2 = requests.get(file_info["raw_url"], headers={"Authorization": f"token {token}"}, timeout=30)
        r2.raise_for_status()
        content = r2.text
    else:
        content = file_info["content"]
    df = pd.read_csv(io.StringIO(content))
    df["ano"] = df["ano"].astype("Int64")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_6anos_from_gist() -> pd.DataFrame:
    """Carrega sales_6anos.csv do Gist (impacto planos de 6 anos por ano)."""
    token = get_secret("GITHUB_GIST_TOKEN")
    gist_id = get_secret("GITHUB_GIST_ID")
    if not token or not gist_id:
        return pd.DataFrame()
    r = requests.get(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    r.raise_for_status()
    file_info = r.json()["files"].get("sales_6anos.csv")
    if not file_info:
        return pd.DataFrame()
    if file_info.get("truncated") or not file_info.get("content"):
        r2 = requests.get(file_info["raw_url"], headers={"Authorization": f"token {token}"}, timeout=30)
        r2.raise_for_status()
        content = r2.text
    else:
        content = file_info["content"]
    df = pd.read_csv(io.StringIO(content))
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_products_from_gist() -> pd.DataFrame:
    """Carrega sales_by_product.csv do Gist (vendas por produto/oferta por ano)."""
    token = get_secret("GITHUB_GIST_TOKEN")
    gist_id = get_secret("GITHUB_GIST_ID")
    if not token or not gist_id:
        return pd.DataFrame()
    r = requests.get(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    r.raise_for_status()
    file_info = r.json()["files"].get("sales_by_product.csv")
    if not file_info:
        return pd.DataFrame()
    if file_info.get("truncated") or not file_info.get("content"):
        r2 = requests.get(file_info["raw_url"], headers={"Authorization": f"token {token}"}, timeout=30)
        r2.raise_for_status()
        content = r2.text
    else:
        content = file_info["content"]
    df = pd.read_csv(io.StringIO(content))
    df["ano"] = df["ano"].astype("Int64")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_recent_hotmart(since_date_str: str) -> list[dict]:
    """Busca vendas Hotmart aprovadas a partir de uma data."""
    since = datetime.strptime(since_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    since_ms = int(since.timestamp() * 1000)
    token = hotmart_get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://developers.hotmart.com/payments/api/v1/sales/history"
    records, page_token = [], None

    while True:
        params = [
            ("max_results", 500),
            ("transaction_status", "APPROVED"),
            ("transaction_status", "COMPLETE"),
            ("start_date", since_ms),
        ]
        if page_token:
            params.append(("page_token", page_token))
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(30)
            continue
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            product_name = ((item.get("product") or {}).get("name") or "").lower()
            if product_name and "medsimple" not in product_name:
                continue
            buyer = item.get("buyer") or {}
            purchase = item.get("purchase") or {}
            phone = "".join(filter(str.isdigit, str(buyer.get("phone") or buyer.get("document") or "")))
            if len(phone) == 11 and not phone.startswith("0"):
                phone = ""
            records.append({
                "email": (buyer.get("email") or "").strip().lower(),
                "telefone": phone,
                "nome": (buyer.get("name") or "").strip(),
                "data_compra": ms_to_dt(purchase.get("approved_date") or purchase.get("order_date")),
                "plataforma": "hotmart",
            })
        pi = data.get("page_info") or {}
        page_token = pi.get("next_page_token")
        if not page_token or not data.get("items"):
            break
        time.sleep(0.4)
    return records


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_recent_guru(since_date_str: str) -> list[dict]:
    """Busca transações Guru (Plataforma MEDsimple) a partir de uma data."""
    from datetime import timedelta
    since = datetime.strptime(since_date_str, "%Y-%m-%d").date()
    today = date.today()
    records = []
    window_start = since
    while window_start <= today:
        window_end = min(window_start + timedelta(days=GURU_WINDOW_DAYS), today)
        try:
            records.extend(guru_fetch_window(window_start, window_end))
        except Exception:
            pass
        window_start = window_end + timedelta(days=1)
    return records


@st.cache_data(ttl=3600, show_spinner=False)
def load_data() -> pd.DataFrame:
    """
    Carrega baseline do Gist e complementa com dados recentes das APIs.
    """
    status_box = st.empty()

    # 1. Baseline histórico
    status_box.info("⏳ Carregando baseline histórico...")
    try:
        df_baseline = load_baseline_from_gist()
    except Exception as e:
        st.warning(f"Baseline: {e}")
        df_baseline = pd.DataFrame()

    # Data de corte para busca incremental
    # Baseline cobre até hoje → API só precisa cobrir os últimos 7 dias
    from datetime import timedelta
    last_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    # 2. Hotmart — dados recentes
    status_box.info(f"⏳ Hotmart: buscando vendas após {last_date}...")
    hotmart_recent = []
    try:
        hotmart_recent = fetch_recent_hotmart(last_date)
    except Exception as e:
        st.warning(f"Hotmart API: {e}")

    # 3. Guru — dados recentes (só Plataforma MEDsimple, filtrado em guru_normalize)
    status_box.info(f"⏳ Guru: buscando transações após {last_date}...")
    guru_recent = []
    try:
        guru_recent = fetch_recent_guru(last_date)
    except Exception as e:
        st.warning(f"Guru API: {e}")

    status_box.empty()

    # 4. Montar DataFrame combinado
    frames = []
    if not df_baseline.empty:
        # Converter baseline para formato de transações brutas para re-processar cruzamento
        base_raw = df_baseline.rename(columns={
            "plataforma_primeira_compra": "plataforma",
            "data_primeira_compra": "data_compra",
        })[["email", "telefone", "nome", "data_compra", "plataforma"]]
        frames.append(base_raw)

    if hotmart_recent:
        frames.append(pd.DataFrame(hotmart_recent))
    if guru_recent:
        frames.append(pd.DataFrame(guru_recent))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["data_compra"])
    df["email"] = df["email"].fillna("")
    df["telefone"] = df["telefone"].fillna("")
    return find_unique_clients(df)


# ── Resumo histórico ────────────────────────────────────────────────────────
def render_summary(df_full, df_sales: pd.DataFrame):
    # Novos clientes por ano
    novos = df_full.groupby("ano").agg(
        Novos=("plataforma_primeira_compra", "count"),
        Hotmart=("plataforma_primeira_compra", lambda x: (x == "hotmart").sum()),
        Guru=("plataforma_primeira_compra", lambda x: (x == "guru").sum()),
    ).reset_index().sort_values("ano")
    novos["ano"] = novos["ano"].astype("Int64")

    # Merge com total de vendas (inclui recompras)
    if not df_sales.empty:
        novos = novos.merge(
            df_sales[["ano", "total"]].rename(columns={"total": "Vendas"}),
            on="ano", how="left",
        )
        novos["Vendas"] = novos["Vendas"].fillna(0).astype(int)
    else:
        novos["Vendas"] = pd.NA

    # Formatar coluna Ano (marcar 2026 como ao vivo)
    ano_atual = pd.Timestamp.now().year
    novos["Ano"] = novos["ano"].apply(
        lambda a: f"{a} 🔴" if int(a) >= ano_atual else str(int(a))
    )

    # Linha de total
    total_data = {
        "Ano": "Total",
        "Novos": int(novos["Novos"].sum()),
        "Hotmart": int(novos["Hotmart"].sum()),
        "Guru": int(novos["Guru"].sum()),
    }
    if "Vendas" in novos.columns and not novos["Vendas"].isna().all():
        total_data["Vendas"] = int(novos["Vendas"].sum())
    resumo = pd.concat(
        [novos[list(total_data.keys())], pd.DataFrame([total_data])],
        ignore_index=True,
    )

    # Colorir via Styler
    def _color(row):
        if row["Ano"] == "Total":
            return ["background-color: #1e293b; color: #f8fafc; font-weight: 700"] * len(row)
        novos_val = row["Novos"] if row["Novos"] else 1
        guru_pct = row["Guru"] / novos_val
        if guru_pct >= 0.8:
            bg = "#e0f2fe"  # azul suave — Guru dominante
        elif guru_pct <= 0.2:
            bg = "#fff7ed"  # âmbar suave — Hotmart dominante
        else:
            bg = "#f0fdf4"  # verde suave — misto
        return [f"background-color: {bg}"] * len(row)

    styled = resumo.style.apply(_color, axis=1)

    cols_config = {
        "Ano": st.column_config.TextColumn("Ano"),
        "Novos": st.column_config.NumberColumn("Novos Clientes", format="%d"),
        "Hotmart": st.column_config.NumberColumn("Hotmart", format="%d"),
        "Guru": st.column_config.NumberColumn("Guru", format="%d"),
    }
    if "Vendas" in resumo.columns:
        cols_config["Vendas"] = st.column_config.NumberColumn("Total Vendas", format="%d",
                                                               help="Todas as transações aprovadas, incluindo recompras")

    st.subheader("Novos clientes por ano")
    st.caption("Clientes únicos históricos — primeira compra em qualquer plataforma · 🔴 = ano corrente (atualiza via API)")
    st.dataframe(styled, hide_index=True, use_container_width=True, column_config=cols_config)


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


# ── Aba Plano 6 Anos ──────────────────────────────────────────────────────
def render_6anos(df_6a: pd.DataFrame):
    """Renderiza a aba de análise dos planos de 6 anos."""
    if df_6a.empty:
        st.info("Dados de planos de 6 anos não disponíveis. Execute publish_baseline.py para gerar.")
        return

    st.subheader("Impacto dos Planos de 6 Anos nas Vendas")
    st.caption("Transações aprovadas cujo nome da oferta contém '6 ano' (Guru) · Hotmart não tem planos de 6 anos")

    # KPIs
    total_6a = int(df_6a["trans_6anos"].sum())
    total_all = int(df_6a["trans_total"].sum())
    rec_6a = df_6a["receita_6anos"].sum()
    rec_all = df_6a["receita_total"].sum()
    pct_trans = total_6a / total_all * 100 if total_all else 0
    pct_rec = rec_6a / rec_all * 100 if rec_all else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Vendas 6 Anos (total)", f"{total_6a:,}".replace(",", "."))
    c2.metric("% das Transações", f"{pct_trans:.1f}%")
    c3.metric("Receita 6 Anos", f"R$ {rec_6a:,.0f}".replace(",", "."))
    c4.metric("% da Receita", f"{pct_rec:.1f}%")

    st.divider()

    # Tabela por ano
    ano_atual = pd.Timestamp.now().year
    display = df_6a.copy()
    display["Ano"] = display["ano"].apply(lambda a: f"{int(a)} 🔴" if int(a) >= ano_atual else str(int(a)))
    display["Vendas 6 Anos"] = display["trans_6anos"].astype(int)
    display["Vendas Demais"] = display["trans_outros"].astype(int)
    display["Total"] = display["trans_total"].astype(int)
    display["% 6 Anos"] = (display["trans_6anos"] / display["trans_total"] * 100).round(1)
    display["Receita 6 Anos (R$)"] = display["receita_6anos"].apply(lambda v: f"{v:,.0f}".replace(",", "."))
    display["Receita Demais (R$)"] = display["receita_outros"].apply(lambda v: f"{v:,.0f}".replace(",", "."))
    display["Receita Total (R$)"] = display["receita_total"].apply(lambda v: f"{v:,.0f}".replace(",", "."))

    show_cols = ["Ano", "Vendas 6 Anos", "Vendas Demais", "Total", "% 6 Anos",
                 "Receita 6 Anos (R$)", "Receita Demais (R$)", "Receita Total (R$)"]

    def _color_6a(row):
        pct = row["% 6 Anos"]
        if pct >= 5:
            bg = "#fef3c7"  # amarelo — impacto significativo
        elif pct >= 2:
            bg = "#f0fdf4"  # verde claro — impacto moderado
        else:
            bg = "#f8fafc"  # cinza claro — baixo impacto
        return [f"background-color: {bg}"] * len(row)

    styled = display[show_cols].style.apply(_color_6a, axis=1)
    st.dataframe(styled, hide_index=True, use_container_width=True)

    # Gráfico
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Transações: 6 Anos vs Demais")
        chart_data = display.set_index("Ano")[["Vendas 6 Anos", "Vendas Demais"]]
        st.bar_chart(chart_data, use_container_width=True)
    with col2:
        st.subheader("% de Vendas — Plano 6 Anos")
        pct_data = display.set_index("Ano")[["% 6 Anos"]]
        st.bar_chart(pct_data, use_container_width=True, color="#f59e0b")


def render_produtos(df_prod: pd.DataFrame):
    """Renderiza a aba de vendas por produto."""
    if df_prod.empty:
        st.info("Dados de vendas por produto não disponíveis. Execute publish_baseline.py para gerar.")
        return

    st.subheader("Vendas por Produto / Oferta")
    st.caption("Hotmart: agrupado por nome do produto · Guru: agrupado por nome da oferta (mais detalhado)")

    # Resumo por produto (todas os anos somados)
    resumo = df_prod.groupby(["produto", "plataforma"]).agg(
        vendas=("vendas", "sum"),
        receita=("receita", "sum"),
    ).reset_index()
    resumo["ticket_medio"] = (resumo["receita"] / resumo["vendas"]).round(2)
    total_vendas = resumo["vendas"].sum()
    total_receita = resumo["receita"].sum()
    resumo["pct_vendas"] = (resumo["vendas"] / total_vendas * 100).round(1)
    resumo["pct_receita"] = (resumo["receita"] / total_receita * 100).round(1)
    resumo = resumo.sort_values("vendas", ascending=False)

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Produtos/Ofertas", f"{resumo['produto'].nunique()}")
    top = resumo.iloc[0]
    c2.metric("Mais Vendido", top["produto"][:30])
    c3.metric("Total Vendas", f"{int(total_vendas):,}".replace(",", "."))
    c4.metric("Receita Total", f"R$ {total_receita:,.0f}".replace(",", "."))

    st.divider()

    # Tabela resumo por produto
    st.subheader("Resumo por Produto")
    display = resumo.copy()
    display["Produto"] = display["produto"]
    display["Plataforma"] = display["plataforma"].str.capitalize()
    display["Vendas"] = display["vendas"].astype(int)
    display["Receita (R$)"] = display["receita"].apply(lambda v: f"{v:,.0f}".replace(",", "."))
    display["Ticket Médio (R$)"] = display["ticket_medio"].apply(lambda v: f"{v:,.0f}".replace(",", "."))
    display["% Vendas"] = display["pct_vendas"]
    display["% Receita"] = display["pct_receita"]

    show_cols = ["Produto", "Plataforma", "Vendas", "Receita (R$)", "Ticket Médio (R$)", "% Vendas", "% Receita"]

    def _color_prod(row):
        pct = row["% Vendas"]
        if pct >= 10:
            bg = "#e0f2fe"  # azul — produto principal
        elif pct >= 3:
            bg = "#f0fdf4"  # verde — relevante
        elif pct >= 1:
            bg = "#fefce8"  # amarelo — moderado
        else:
            bg = "#f8fafc"  # cinza — baixo volume
        return [f"background-color: {bg}"] * len(row)

    styled = display[show_cols].style.apply(_color_prod, axis=1)
    st.dataframe(styled, hide_index=True, use_container_width=True)

    st.divider()

    # Gráfico barras horizontais - Top 10 por vendas
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 — Por Volume de Vendas")
        top10 = resumo.head(10).set_index("produto")[["vendas"]].rename(columns={"vendas": "Vendas"})
        st.bar_chart(top10, use_container_width=True, horizontal=True)
    with col2:
        st.subheader("Top 10 — Por Receita")
        top10r = resumo.head(10).set_index("produto")[["receita"]].rename(columns={"receita": "Receita (R$)"})
        st.bar_chart(top10r, use_container_width=True, horizontal=True, color="#10b981")

    st.divider()

    # Evolução por ano dos top 5 produtos
    st.subheader("Evolução Anual — Top 5 Produtos")
    top5 = resumo.head(5)["produto"].tolist()
    evol = df_prod[df_prod["produto"].isin(top5)].pivot_table(
        index="ano", columns="produto", values="vendas", aggfunc="sum", fill_value=0
    )
    st.bar_chart(evol, use_container_width=True)


def main():
    st.title("📊 Novos Clientes — Medsimple")

    df_full = load_data()

    if df_full.empty:
        st.error("Sem dados. Verifique GITHUB_GIST_TOKEN e GITHUB_GIST_ID nos secrets.")
        return

    try:
        df_sales = load_sales_from_gist()
    except Exception:
        df_sales = pd.DataFrame()

    try:
        df_6anos = load_6anos_from_gist()
    except Exception:
        df_6anos = pd.DataFrame()

    try:
        df_produtos = load_products_from_gist()
    except Exception:
        df_produtos = pd.DataFrame()

    tab1, tab2, tab3 = st.tabs(["Novos Clientes", "Plano 6 Anos", "Vendas por Produto"])

    with tab1:
        st.caption("Primeira compra em qualquer plataforma (Hotmart + Guru cruzados por email/telefone)")
        render_summary(df_full, df_sales)
        st.divider()

        with st.sidebar:
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
            if st.button("🔄 Recarregar API", use_container_width=True):
                st.cache_data.clear()
                st.rerun()

        df_filtered = apply_filters(df_full, filtro_ano, filtro_mes, filtro_plataforma, filtro_periodo)
        render_kpis(df_filtered, df_full)
        render_charts(df_filtered)
        render_table(df_filtered)

    with tab2:
        render_6anos(df_6anos)

    with tab3:
        render_produtos(df_produtos)


main()
