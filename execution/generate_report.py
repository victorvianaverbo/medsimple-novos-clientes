"""
generate_report.py
Gera relatorio_medsimple.pdf com LTV, renovacao e retencao por coorte.

Requer: pip install fpdf2 pandas
Uso: python execution/generate_report.py
Saida: relatorio_medsimple.pdf (na raiz do projeto)
"""

import os
import warnings
import pandas as pd
from datetime import datetime
from fpdf import FPDF

warnings.filterwarnings("ignore", category=DeprecationWarning)

INPUT_FILE   = os.path.join(os.path.dirname(__file__), "..", ".tmp", "new_clients.csv")
HOTMART_RAW  = os.path.join(os.path.dirname(__file__), "..", ".tmp", "hotmart_raw.csv")
GURU_RAW     = os.path.join(os.path.dirname(__file__), "..", ".tmp", "guru_raw.csv")
OUTPUT_FILE  = os.path.join(os.path.dirname(__file__), "..", "relatorio_medsimple.pdf")


def load_data():
    df = pd.read_csv(INPUT_FILE, dtype=str)
    df["ltv"] = pd.to_numeric(df.get("ltv"), errors="coerce").fillna(0)
    df["n_compras"] = pd.to_numeric(df.get("n_compras"), errors="coerce").fillna(1).astype(int)
    df["data_primeira_compra"] = pd.to_datetime(df["data_primeira_compra"], errors="coerce", utc=True)
    df["data_ultima_compra"] = pd.to_datetime(df.get("data_ultima_compra"), errors="coerce", utc=True)
    df["ano"] = df["data_primeira_compra"].dt.year
    return df


FILTRO_6ANOS = "6 ano"  # match para "6 anos", "6 Anos", "Plano 6 Anos" etc.


def load_raw_sales():
    """Carrega transacoes brutas (hotmart + guru) para calcular receita real por ano."""
    dfs = []
    for path, plat in [(HOTMART_RAW, "hotmart"), (GURU_RAW, "guru")]:
        if os.path.exists(path):
            d = pd.read_csv(path, dtype=str)
            d["plataforma"] = plat
            d["valor_liquido"] = pd.to_numeric(d.get("valor_liquido", 0), errors="coerce").fillna(0)
            d["data_compra"] = pd.to_datetime(d["data_compra"], errors="coerce", utc=True)
            d = d.dropna(subset=["data_compra"])
            d["ano"] = d["data_compra"].dt.year
            if "nome_produto" not in d.columns:
                d["nome_produto"] = ""
            else:
                d["nome_produto"] = d["nome_produto"].fillna("")
            dfs.append(d)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def section_title(pdf, text):
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_fill_color(30, 80, 160)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 9, text, ln=True, fill=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)


def table_header(pdf, cols):
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(220, 230, 245)
    for label, w in cols:
        pdf.cell(w, 7, label, border=1, align="C", fill=True)
    pdf.ln()


def table_row(pdf, vals, cols, fill=False):
    pdf.set_font("Helvetica", "", 8)
    if fill:
        pdf.set_fill_color(245, 248, 255)
    for val, (_, w) in zip(vals, cols):
        pdf.cell(w, 6, str(val), border=1, align="C", fill=fill)
    pdf.ln()


def safe(text):
    """Remove caracteres fora do range latin-1 que o Helvetica nao suporta."""
    return (text
            .replace("\u2014", "-").replace("\u2013", "-")
            .replace("\u2018", "'").replace("\u2019", "'")
            .replace("\u201c", '"').replace("\u201d", '"'))


def bullet(pdf, text):
    """Escreve um item de lista com marcador."""
    pdf.set_font("Helvetica", "", 9)
    pdf.set_x(pdf.l_margin + 4)
    pdf.multi_cell(0, 5, safe(f"- {text}"))


def paragraph(pdf, text):
    """Escreve um paragrafo justificado."""
    pdf.set_font("Helvetica", "", 9)
    pdf.multi_cell(0, 5, safe(text))
    pdf.ln(1)


def subsection(pdf, text):
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(30, 80, 160)
    pdf.cell(0, 7, text, ln=True)
    pdf.set_text_color(0, 0, 0)


def fmt_r(v):
    """Formata valor monetario BRL com ponto como separador de milhar."""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_r0(v):
    return f"R$ {v:,.0f}".replace(",", ".")


def fmt_n(v):
    return f"{int(v):,}".replace(",", ".")


def main():
    print("Carregando dados...")
    df = load_data()
    df_raw = load_raw_sales()
    total = len(df)

    # KPIs globais
    ltv_medio = df["ltv"].mean()
    df_ren = df[df["n_compras"] > 1]
    taxa_ren = len(df_ren) / total * 100
    perm_dias = (df_ren["data_ultima_compra"] - df_ren["data_primeira_compra"]).dt.days
    perm_meses = perm_dias.mean() / 30.44 if len(df_ren) > 0 else 0
    receita_total = df["ltv"].sum()
    df["ano_ultima"] = df["data_ultima_compra"].dt.year

    # Metricas por ano (receita REAL das transacoes brutas)
    anos_sorted = sorted(df["ano"].dropna().unique())
    ano_stats = {}
    for ano in anos_sorted:
        g = df[df["ano"] == ano]
        hm = len(g[g["plataforma_primeira_compra"] == "hotmart"])
        gu = len(g[g["plataforma_primeira_compra"] == "guru"])
        g_ren = g[g["n_compras"] > 1]
        perm_a = (g_ren["data_ultima_compra"] - g_ren["data_primeira_compra"]).dt.days.mean() / 30.44 if len(g_ren) > 0 else 0
        ano_stats[ano] = {
            "novos": len(g), "hotmart": hm, "guru": gu,
            "ltv_medio": g["ltv"].mean(), "ren": len(g_ren) / len(g) * 100,
            "perm": perm_a,
        }

    # Receita real por ano e por plataforma (todas as transacoes, nao so primeiras)
    raw_ano = {}
    raw_plat = {}
    if not df_raw.empty:
        for ano in df_raw["ano"].dropna().unique():
            g = df_raw[df_raw["ano"] == ano]
            raw_ano[ano] = {"receita": g["valor_liquido"].sum(), "transacoes": len(g),
                            "ticket": g["valor_liquido"].mean()}
        for plat in ["hotmart", "guru"]:
            g = df_raw[df_raw["plataforma"] == plat]
            raw_plat[plat] = {"receita": g["valor_liquido"].sum(), "transacoes": len(g),
                              "ticket": g["valor_liquido"].mean()}

    receita_real_total = sum(v["receita"] for v in raw_ano.values()) if raw_ano else receita_total

    # ── PDF ──────────────────────────────────────────────────────
    pdf = FPDF()
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Cabecalho
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(30, 80, 160)
    pdf.cell(0, 12, "MEDsimple - Relatorio de Clientes", ln=True, align="C")
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, f"Gerado em {datetime.today().strftime('%d/%m/%Y %H:%M')}", ln=True, align="C")
    pdf.set_text_color(0, 0, 0)
    pdf.ln(6)

    # ── Secao 1: Metricas Globais ──────────────────────────────
    section_title(pdf, "1. Metricas Globais")
    pdf.set_font("Helvetica", "", 10)

    kpis = [
        ("Total de clientes unicos", f"{total:,}".replace(",", ".")),
        ("Receita liquida total acumulada", f"R$ {receita_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")),
        ("LTV medio por cliente", f"R$ {ltv_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")),
        ("Taxa de renovacao (recompra)", f"{taxa_ren:.1f}%  ({len(df_ren):,} clientes com mais de 1 compra)".replace(",", ".")),
        ("Permanencia media (renovantes)", f"{perm_meses:.1f} meses entre 1a e ultima compra"),
    ]
    for label, valor in kpis:
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(85, 7, label + ":", border=0)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 7, valor, ln=True)
    pdf.ln(4)

    # ── Secao 2: Novos Clientes por Ano ───────────────────────
    section_title(pdf, "2. Novos Clientes por Ano de Aquisicao")
    cols2 = [
        ("Ano", 14), ("Novos", 20), ("Hotmart", 22), ("Guru", 18),
        ("Receita (R$)", 36), ("LTV Medio", 32), ("Renovacao", 24), ("Perm. Media", 28),
    ]
    table_header(pdf, cols2)

    for i, ano in enumerate(sorted(df["ano"].dropna().unique())):
        g = df[df["ano"] == ano]
        hm = len(g[g["plataforma_primeira_compra"] == "hotmart"])
        gu = len(g[g["plataforma_primeira_compra"] == "guru"])
        rec_a = g["ltv"].sum()
        ltv_a = g["ltv"].mean()
        ren_a = len(g[g["n_compras"] > 1]) / len(g) * 100
        g_ren = g[g["n_compras"] > 1]
        perm_a = (g_ren["data_ultima_compra"] - g_ren["data_primeira_compra"]).dt.days.mean() / 30.44 if len(g_ren) > 0 else 0
        vals = [
            str(int(ano)), str(len(g)), str(hm), str(gu),
            f"{rec_a:,.0f}".replace(",", "."),
            f"{ltv_a:,.0f}".replace(",", "."),
            f"{ren_a:.0f}%",
            f"{perm_a:.1f} m",
        ]
        table_row(pdf, vals, cols2, fill=(i % 2 == 0))
    pdf.ln(5)

    # ── Secao 3: Retencao por Coorte ──────────────────────────
    section_title(pdf, "3. Taxa de Retencao por Coorte de Aquisicao")
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 5,
             "% de clientes que realizaram ao menos mais uma compra nos anos seguintes ao da aquisicao",
             ln=True)
    pdf.ln(2)

    cols3 = [
        ("Coorte", 18), ("Clientes", 22), ("Ret. ano+1", 30),
        ("Ret. ano+2", 30), ("Ret. ano+3", 30), ("LTV Medio", 30),
    ]
    table_header(pdf, cols3)

    for i, ano in enumerate(sorted(df["ano"].dropna().unique())):
        g = df[df["ano"] == ano]
        n = len(g)
        r1 = len(g[g["ano_ultima"] >= ano + 1])
        r2 = len(g[g["ano_ultima"] >= ano + 2])
        r3 = len(g[g["ano_ultima"] >= ano + 3])
        ltv_c = g["ltv"].mean()
        vals = [
            str(int(ano)), str(n),
            f"{r1/n*100:.0f}% ({r1})",
            f"{r2/n*100:.0f}% ({r2})",
            f"{r3/n*100:.0f}% ({r3})",
            f"R$ {ltv_c:,.0f}".replace(",", "."),
        ]
        table_row(pdf, vals, cols3, fill=(i % 2 == 0))

    pdf.ln(5)

    # ── Secao 4: Analise Financeira ───────────────────────────
    pdf.add_page()
    section_title(pdf, "4. Analise Financeira")

    # --- 4.1 Crescimento da Base ---
    subsection(pdf, "4.1 Crescimento da Base de Clientes")

    anos_list = [a for a in anos_sorted if a in ano_stats]
    pico_ano = max(anos_list, key=lambda a: ano_stats[a]["novos"])
    pico_val = ano_stats[pico_ano]["novos"]
    val_2021 = ano_stats.get(2021, {}).get("novos", 0)
    val_2024 = ano_stats.get(2024, {}).get("novos", 0)
    val_2025 = ano_stats.get(2025, {}).get("novos", 0)
    val_2026 = ano_stats.get(2026, {}).get("novos", 0)
    cresc_21_23 = ((ano_stats.get(2023, {}).get("novos", 0) / val_2021) - 1) * 100 if val_2021 else 0
    var_24_25 = ((val_2025 / val_2024) - 1) * 100 if val_2024 else 0

    paragraph(pdf,
        f"A base de novos clientes cresceu de forma expressiva entre 2021 e 2023, acumulando um aumento "
        f"de {cresc_21_23:.0f}% no periodo — de {fmt_n(val_2021)} para {fmt_n(ano_stats.get(2023,{}).get('novos',0))} novos clientes ao ano. "
        f"O pico de aquisicao ocorreu em {int(pico_ano)}, com {fmt_n(pico_val)} novos clientes.")

    paragraph(pdf,
        f"A partir de 2024, observa-se uma desaceleracao: {fmt_n(val_2024)} novos clientes em 2024 e "
        f"{fmt_n(val_2025)} em 2025, representando uma queda de {abs(var_24_25):.0f}% de um ano para o outro. "
        f"Parte dessa reducao e explicada pela transicao de plataforma (Hotmart para Guru), que pode ter "
        f"gerado descontinuidade temporaria na captacao. Os {fmt_n(val_2026)} novos ja registrados em 2026 "
        f"(apenas 3 meses) sugerem ritmo anualizado proximo a {fmt_n(val_2026 * 4)} clientes/ano — "
        f"alinhado ao nivel de 2025.")
    pdf.ln(2)

    # --- 4.2 Receita e Ticket Medio ---
    subsection(pdf, "4.2 Receita e Ticket Medio por Plataforma")

    hm_ticket = raw_plat.get("hotmart", {}).get("ticket", 0)
    gu_ticket = raw_plat.get("guru", {}).get("ticket", 0)
    hm_rec = raw_plat.get("hotmart", {}).get("receita", 0)
    gu_rec = raw_plat.get("guru", {}).get("receita", 0)
    hm_trans = raw_plat.get("hotmart", {}).get("transacoes", 0)
    gu_trans = raw_plat.get("guru", {}).get("transacoes", 0)
    total_trans = hm_trans + gu_trans

    paragraph(pdf,
        f"A receita liquida total acumulada e de {fmt_r(receita_real_total)}, distribuida entre "
        f"{fmt_n(total_trans)} transacoes aprovadas no historico completo.")

    bullet(pdf,
        f"Hotmart: {fmt_n(hm_trans)} transacoes | Receita total {fmt_r0(hm_rec)} | "
        f"Ticket medio {fmt_r(hm_ticket)}/transacao (cobranca mensal recorrente)")
    bullet(pdf,
        f"Guru: {fmt_n(gu_trans)} transacoes | Receita total {fmt_r0(gu_rec)} | "
        f"Ticket medio {fmt_r(gu_ticket)}/transacao (plano anual parcelado)")
    pdf.ln(2)

    paragraph(pdf,
        f"O ticket medio da Guru e significativamente maior que o da Hotmart, o que reflete a diferenca "
        f"nos modelos de cobranca: a Hotmart opera com assinaturas mensais (cada transacao = 1 mes), "
        f"enquanto a Guru cobra planos anuais parcelados (cada transacao representa o valor de um ciclo "
        f"anual). Portanto, a comparacao direta de ticket medio entre plataformas requer normalizacao "
        f"pelo periodo de vigencia do plano.")
    pdf.ln(2)

    # --- 4.3 Transicao de Plataformas ---
    subsection(pdf, "4.3 Transicao Hotmart para Guru")

    hm_2024 = ano_stats.get(2024, {}).get("hotmart", 0)
    hm_2025 = ano_stats.get(2025, {}).get("hotmart", 0)
    gu_2024 = ano_stats.get(2024, {}).get("guru", 0)
    gu_2025 = ano_stats.get(2025, {}).get("guru", 0)
    var_hm = ((hm_2025 / hm_2024) - 1) * 100 if hm_2024 else 0
    var_gu = ((gu_2025 / gu_2024) - 1) * 100 if gu_2024 else 0

    paragraph(pdf,
        f"O movimento mais relevante do periodo e a migracao da plataforma de vendas de Hotmart para Guru, "
        f"acelerada em 2025. Os novos clientes pela Hotmart caíram de {fmt_n(hm_2024)} (2024) para "
        f"{fmt_n(hm_2025)} (2025) — reducao de {abs(var_hm):.0f}%. No mesmo periodo, os novos clientes "
        f"pela Guru saltaram de {fmt_n(gu_2024)} para {fmt_n(gu_2025)} — crescimento de {var_gu:.0f}%. "
        f"Em 2026, 100% das aquisicoes ja passam pela Guru, indicando que a transicao esta concluida.")

    paragraph(pdf,
        f"Essa mudanca traz implicacoes financeiras relevantes: o ciclo de cobranca diferente entre "
        f"plataformas pode alterar o fluxo de caixa e o LTV aparente por cliente. Clientes Guru tendem "
        f"a ter LTV registrado maior por transacao, mas e preciso monitorar a taxa de renovacao anual "
        f"para confirmar se esse valor se converte em receita recorrente de longo prazo.")
    pdf.ln(2)

    # --- 4.4 Saude da Retencao ---
    subsection(pdf, "4.4 Saude da Retencao e Fidelizacao")

    r2022_1 = len(df[(df["ano"] == 2022) & (df["ano_ultima"] >= 2023)]) / max(ano_stats.get(2022, {}).get("novos", 1), 1) * 100
    r2022_2 = len(df[(df["ano"] == 2022) & (df["ano_ultima"] >= 2024)]) / max(ano_stats.get(2022, {}).get("novos", 1), 1) * 100
    r2022_3 = len(df[(df["ano"] == 2022) & (df["ano_ultima"] >= 2025)]) / max(ano_stats.get(2022, {}).get("novos", 1), 1) * 100
    r2023_1 = len(df[(df["ano"] == 2023) & (df["ano_ultima"] >= 2024)]) / max(ano_stats.get(2023, {}).get("novos", 1), 1) * 100
    r2023_2 = len(df[(df["ano"] == 2023) & (df["ano_ultima"] >= 2025)]) / max(ano_stats.get(2023, {}).get("novos", 1), 1) * 100

    paragraph(pdf,
        f"A taxa de renovacao global e de {taxa_ren:.1f}% — ou seja, apenas {taxa_ren:.1f}% dos clientes "
        f"realizaram mais de uma compra no historico. Esse numero, analisado isoladamente, pode parecer "
        f"baixo, mas e coerente com um produto de assinatura mensal onde o churn e contabilizado de "
        f"forma diferente de produtos pontuais.")

    bullet(pdf, f"Coorte 2022: {r2022_1:.0f}% ainda ativos em 2023 | {r2022_2:.0f}% em 2024 | {r2022_3:.0f}% em 2025")
    bullet(pdf, f"Coorte 2023: {r2023_1:.0f}% ainda ativos em 2024 | {r2023_2:.0f}% em 2025")
    pdf.ln(2)

    paragraph(pdf,
        f"A permanencia media de {perm_meses:.1f} meses entre primeira e ultima compra (para renovantes) "
        f"indica que os clientes que recompram tendem a manter um relacionamento de longo prazo com a "
        f"plataforma. E um sinal positivo de fidelizacao entre o publico que permanece ativo.")
    pdf.ln(2)

    # --- 4.5 Pontos de Atencao ---
    subsection(pdf, "4.5 Pontos de Atencao e Recomendacoes")

    bullet(pdf,
        f"Queda de captacao 2024-2025 (-{abs(var_24_25):.0f}%): monitorar se e efeito de transicao de "
        f"plataforma ou reducao estrutural de demanda. Recomenda-se analise de CAC e investimento em marketing.")
    bullet(pdf,
        f"Taxa de renovacao de {taxa_ren:.1f}% e baixa para um produto SaaS/educacional. Estrategias de "
        f"engajamento e upsell podem elevar esse indicador e o LTV medio significativamente.")
    bullet(pdf,
        f"LTV medio de {fmt_r(ltv_medio)} por cliente: com {fmt_n(total)} clientes na base, ha "
        f"potencial significativo de receita incremental com campanhas de reativacao.")
    bullet(pdf,
        f"2026 em andamento (apenas 3 meses de dados): os numeros de 2026 devem ser interpretados "
        f"com cautela — projecao anual sujeita a sazonalidade.")
    bullet(pdf,
        f"Transicao Hotmart-Guru concluida: consolidar metricas de retencao especificas para "
        f"assinaturas anuais (churn anual) para avaliar a saude da base na nova plataforma.")

    pdf.ln(4)

    # ── Secao 5: Planos de 6 Anos vs Demais ───────────────────
    pdf.add_page()
    section_title(pdf, "5. Comparativo: Planos de 6 Anos vs Demais (2024-2026)")

    if not df_raw.empty and "nome_produto" in df_raw.columns:
        # Identificar produtos unicos para referencia
        produtos_unicos = sorted(df_raw["nome_produto"].dropna().unique())
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(100, 100, 100)
        produtos_str = " | ".join([p for p in produtos_unicos if p][:10])
        pdf.multi_cell(0, 4, safe(f"Produtos na base: {produtos_str}"))
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)

        mask_6a = df_raw["nome_produto"].str.lower().str.contains(FILTRO_6ANOS, na=False)
        df_6a = df_raw[mask_6a]
        df_outros = df_raw[~mask_6a]

        anos_comp = [2024, 2025, 2026]

        # --- Tabela Volume (quantidade de transacoes) ---
        subsection(pdf, "5.1 Volume de Vendas (transacoes aprovadas)")
        cols5v = [("Ano", 16), ("6 Anos - Qtd", 36), ("6 Anos - %", 28),
                  ("Demais - Qtd", 36), ("Demais - %", 28), ("Total", 30)]
        table_header(pdf, cols5v)

        for i, ano in enumerate(anos_comp):
            q6 = len(df_6a[df_6a["ano"] == ano])
            qo = len(df_outros[df_outros["ano"] == ano])
            tot = q6 + qo
            p6 = q6 / tot * 100 if tot > 0 else 0
            po = qo / tot * 100 if tot > 0 else 0
            vals = [str(ano), fmt_n(q6), f"{p6:.1f}%", fmt_n(qo), f"{po:.1f}%", fmt_n(tot)]
            table_row(pdf, vals, cols5v, fill=(i % 2 == 0))

        # Total geral 2024-2026
        q6t = sum(len(df_6a[df_6a["ano"] == a]) for a in anos_comp)
        qot = sum(len(df_outros[df_outros["ano"] == a]) for a in anos_comp)
        tott = q6t + qot
        p6t = q6t / tott * 100 if tott > 0 else 0
        pot = qot / tott * 100 if tott > 0 else 0
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(200, 215, 240)
        for val, (_, w) in zip(["TOTAL", fmt_n(q6t), f"{p6t:.1f}%", fmt_n(qot), f"{pot:.1f}%", fmt_n(tott)], cols5v):
            pdf.cell(w, 6, val, border=1, align="C", fill=True)
        pdf.ln()
        pdf.ln(4)

        # --- Tabela Faturamento ---
        subsection(pdf, "5.2 Faturamento Liquido (R$)")
        cols5f = [("Ano", 16), ("6 Anos - R$", 40), ("6 Anos - %", 28),
                  ("Demais - R$", 40), ("Demais - %", 28), ("Total R$", 22)]
        table_header(pdf, cols5f)

        for i, ano in enumerate(anos_comp):
            r6 = df_6a[df_6a["ano"] == ano]["valor_liquido"].sum()
            ro = df_outros[df_outros["ano"] == ano]["valor_liquido"].sum()
            rt = r6 + ro
            pr6 = r6 / rt * 100 if rt > 0 else 0
            pro = ro / rt * 100 if rt > 0 else 0
            vals = [str(ano),
                    f"{r6:,.0f}".replace(",", "."), f"{pr6:.1f}%",
                    f"{ro:,.0f}".replace(",", "."), f"{pro:.1f}%",
                    f"{rt:,.0f}".replace(",", ".")]
            table_row(pdf, vals, cols5f, fill=(i % 2 == 0))

        # Total geral
        r6t = sum(df_6a[df_6a["ano"] == a]["valor_liquido"].sum() for a in anos_comp)
        rot = sum(df_outros[df_outros["ano"] == a]["valor_liquido"].sum() for a in anos_comp)
        rtt = r6t + rot
        pr6t = r6t / rtt * 100 if rtt > 0 else 0
        prot = rot / rtt * 100 if rtt > 0 else 0
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(200, 215, 240)
        for val, (_, w) in zip(["TOTAL",
                                 f"{r6t:,.0f}".replace(",", "."), f"{pr6t:.1f}%",
                                 f"{rot:,.0f}".replace(",", "."), f"{prot:.1f}%",
                                 f"{rtt:,.0f}".replace(",", ".")], cols5f):
            pdf.cell(w, 6, val, border=1, align="C", fill=True)
        pdf.ln()
        pdf.ln(4)

        # --- Tabela Ticket Medio ---
        subsection(pdf, "5.3 Ticket Medio por Transacao (R$)")
        cols5t = [("Ano", 16), ("Ticket - 6 Anos", 50), ("Ticket - Demais", 50), ("Ticket - Total", 50)]
        table_header(pdf, cols5t)

        for i, ano in enumerate(anos_comp):
            df6a = df_6a[df_6a["ano"] == ano]
            dfoa = df_outros[df_outros["ano"] == ano]
            dfta = df_raw[df_raw["ano"] == ano]
            tk6 = df6a["valor_liquido"].mean() if len(df6a) > 0 else 0
            tko = dfoa["valor_liquido"].mean() if len(dfoa) > 0 else 0
            tkt = dfta["valor_liquido"].mean() if len(dfta) > 0 else 0
            vals = [str(ano),
                    f"R$ {tk6:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    f"R$ {tko:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    f"R$ {tkt:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")]
            table_row(pdf, vals, cols5t, fill=(i % 2 == 0))

        pdf.ln(4)

        # Nota sobre filtro
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(100, 100, 100)
        pdf.multi_cell(0, 4, safe(
            f"Filtro '6 anos': transacoes cujo nome de produto contem '{FILTRO_6ANOS}' (case-insensitive). "
            f"'Demais' inclui todos os outros planos (mensais, anuais, vitalicios, etc.). "
            f"Os valores sao das transacoes brutas aprovadas — nao deduplicadas por cliente."
        ))
        pdf.set_text_color(0, 0, 0)
    else:
        paragraph(pdf,
            "Dados de nome de produto nao disponiveis. Execute novamente process_hotmart_csv.py e "
            "process_guru_xlsx.py para gerar a nova versao do hotmart_raw.csv e guru_raw.csv com a "
            "coluna nome_produto.")

    pdf.ln(4)

    # Nota de rodape
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(120, 120, 120)
    pdf.multi_cell(0, 4,
        "Notas: LTV = soma do faturamento liquido de todas as compras por cliente (Hotmart: Faturamento Liquido; "
        "Guru: Valor Liquido). Retencao calculada pela data da ultima compra registrada nos dados historicos. "
        "Clientes cruzados por email e/ou telefone entre plataformas."
    )

    pdf.output(OUTPUT_FILE)
    print(f"\n[OK] Relatorio salvo em: {os.path.abspath(OUTPUT_FILE)}")
    print(f"\nResumo:")
    print(f"  Clientes unicos : {total:,}".replace(",", "."))
    print(f"  LTV medio       : R$ {ltv_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  Taxa renovacao  : {taxa_ren:.1f}%")
    print(f"  Permanencia     : {perm_meses:.1f} meses")


if __name__ == "__main__":
    main()
