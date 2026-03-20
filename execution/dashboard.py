"""
dashboard.py
Dashboard interativo de Novos Clientes — Hotmart + Guru

Como rodar:
  streamlit run execution/dashboard.py

Filtros disponíveis:
  - Ano (Todos, 2024, 2025, ...)
  - Mês (Todos, Janeiro ... Dezembro)
  - Período personalizado (date range)
  - Plataforma (Todas, Hotmart, Guru)
"""

import os
import pandas as pd
import streamlit as st
from datetime import date

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", ".tmp", "new_clients.csv")

MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

st.set_page_config(
    page_title="Novos Clientes — Medsimple",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=300)  # cache de 5 min — recarrega se rodar os scripts de fetch
def load_data():
    if not os.path.exists(DATA_FILE):
        return pd.DataFrame()
    df = pd.read_csv(DATA_FILE, dtype=str)
    df["data_primeira_compra"] = pd.to_datetime(df["data_primeira_compra"], errors="coerce")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    return df


def apply_filters(df, filtro_ano, filtro_mes, filtro_plataforma, filtro_periodo):
    """Aplica os filtros da sidebar no DataFrame."""
    if df.empty:
        return df

    # Filtro de período (date range) — tem prioridade sobre ano/mês
    if filtro_periodo:
        start, end = filtro_periodo
        df = df[
            (df["data_primeira_compra"].dt.date >= start) &
            (df["data_primeira_compra"].dt.date <= end)
        ]
    else:
        if filtro_ano != "Todos":
            df = df[df["ano"] == int(filtro_ano)]
        if filtro_mes != "Todos":
            mes_num = next(k for k, v in MESES.items() if v == filtro_mes)
            df = df[df["mes"] == mes_num]

    if filtro_plataforma != "Todas":
        df = df[df["plataforma_primeira_compra"] == filtro_plataforma.lower()]

    return df


def render_kpis(df, df_full):
    """Exibe os KPIs principais."""
    col1, col2, col3, col4 = st.columns(4)

    total = len(df)
    hotmart = len(df[df["plataforma_primeira_compra"] == "hotmart"])
    guru = len(df[df["plataforma_primeira_compra"] == "guru"])
    total_historico = len(df_full)

    col1.metric("Total no período", f"{total:,}".replace(",", "."))
    col2.metric("Novos via Hotmart", f"{hotmart:,}".replace(",", "."))
    col3.metric("Novos via Guru", f"{guru:,}".replace(",", "."))
    col4.metric("Total histórico", f"{total_historico:,}".replace(",", "."))


def render_charts(df):
    """Exibe gráficos de evolução e comparação."""
    if df.empty:
        st.info("Nenhum dado para exibir com os filtros selecionados.")
        return

    st.divider()

    # Preparar dados mensais
    df_chart = df.copy()
    df_chart["mes_ano"] = df_chart["data_primeira_compra"].dt.to_period("M").astype(str)

    monthly = df_chart.groupby(["mes_ano", "plataforma_primeira_compra"]).size().reset_index(name="count")
    monthly_pivot = monthly.pivot(index="mes_ano", columns="plataforma_primeira_compra", values="count").fillna(0)
    monthly_pivot = monthly_pivot.sort_index()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Evolução mensal")
        totals = df_chart.groupby("mes_ano").size().reset_index(name="Novos Clientes")
        totals = totals.set_index("mes_ano").sort_index()
        st.line_chart(totals, use_container_width=True)

    with col2:
        st.subheader("Hotmart vs Guru por mês")
        st.bar_chart(monthly_pivot, use_container_width=True)


def render_table(df):
    """Exibe tabela de clientes com busca."""
    st.divider()
    st.subheader(f"Clientes ({len(df):,} registros)")

    search = st.text_input("Buscar por email ou nome", placeholder="ex: joao@email.com")
    if search:
        mask = (
            df["email"].str.contains(search, case=False, na=False) |
            df["nome"].str.contains(search, case=False, na=False) |
            df["telefone"].str.contains(search, case=False, na=False)
        )
        df = df[mask]

    display_cols = ["nome", "email", "telefone", "data_primeira_compra", "plataforma_primeira_compra", "ano", "mes"]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols].sort_values("data_primeira_compra", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "nome": st.column_config.TextColumn("Nome"),
            "email": st.column_config.TextColumn("Email"),
            "telefone": st.column_config.TextColumn("Telefone"),
            "data_primeira_compra": st.column_config.DatetimeColumn("Primeira Compra", format="DD/MM/YYYY"),
            "plataforma_primeira_compra": st.column_config.TextColumn("Plataforma"),
            "ano": st.column_config.NumberColumn("Ano"),
            "mes": st.column_config.NumberColumn("Mês"),
        },
    )

    # Botão de download
    csv = df[display_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar CSV",
        data=csv,
        file_name="novos_clientes_filtrado.csv",
        mime="text/csv",
    )


def main():
    st.title("📊 Novos Clientes — Medsimple")
    st.caption("Clientes que compraram pela primeira vez (Hotmart + Guru combinados)")

    df_full = load_data()

    if df_full.empty:
        st.error(
            "Arquivo de dados não encontrado. Execute os scripts na ordem:\n\n"
            "```\n"
            "python execution/fetch_hotmart.py\n"
            "python execution/fetch_guru.py\n"
            "python execution/identify_new_clients.py\n"
            "```"
        )
        return

    # ── Sidebar de filtros ──────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filtros")

        anos_disponiveis = ["Todos"] + sorted(
            [str(a) for a in df_full["ano"].dropna().unique()], reverse=True
        )
        filtro_ano = st.selectbox("Ano", anos_disponiveis)

        filtro_mes = st.selectbox("Mês", ["Todos"] + list(MESES.values()))

        st.divider()
        usar_periodo = st.checkbox("Usar período personalizado")
        filtro_periodo = None
        if usar_periodo:
            col1, col2 = st.columns(2)
            data_min = df_full["data_primeira_compra"].min().date()
            data_max = df_full["data_primeira_compra"].max().date()
            start = col1.date_input("De", value=date(2024, 1, 1), min_value=data_min, max_value=data_max)
            end = col2.date_input("Até", value=data_max, min_value=data_min, max_value=data_max)
            filtro_periodo = (start, end)

        st.divider()
        filtro_plataforma = st.radio("Plataforma", ["Todas", "Hotmart", "Guru"])

        st.divider()
        if st.button("Atualizar dados das APIs", use_container_width=True):
            st.info(
                "Execute manualmente no terminal:\n\n"
                "```\npython execution/fetch_hotmart.py\n"
                "python execution/fetch_guru.py\n"
                "python execution/identify_new_clients.py\n```"
            )
            st.cache_data.clear()

    # ── Aplicar filtros ─────────────────────────────────────────────────────
    df_filtered = apply_filters(df_full, filtro_ano, filtro_mes, filtro_plataforma, filtro_periodo)

    # ── Renderizar ──────────────────────────────────────────────────────────
    render_kpis(df_filtered, df_full)
    render_charts(df_filtered)
    render_table(df_filtered)


if __name__ == "__main__":
    main()
