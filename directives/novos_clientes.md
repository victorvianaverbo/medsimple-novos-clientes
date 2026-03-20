# Diretiva: Novos Clientes — Hotmart + Guru

## Objetivo

Identificar quantos **novos clientes** (primeira compra em qualquer plataforma) tivemos por período.
Cruzamento por email (prioridade) e telefone como fallback.

## Entradas

- Credenciais no `.env`: `HOTMART_CLIENT_ID`, `HOTMART_CLIENT_SECRET`, `HOTMART_BASIC`, `GURU_TOKEN`

## Fluxo de Execução

```
1. python execution/fetch_hotmart.py     → .tmp/hotmart_raw.csv
2. python execution/fetch_guru.py        → .tmp/guru_raw.csv
3. python execution/identify_new_clients.py → .tmp/new_clients.csv (+ resumo no terminal)
4. streamlit run execution/dashboard.py  → dashboard no browser (localhost:8501)
```

## Definição de "Novo Cliente"

Pessoa cuja **primeira compra registrada em qualquer plataforma** ocorreu no período analisado.
Se a mesma pessoa comprou em Hotmart em 2022 e na Guru em 2024, ela **não** é nova em 2024.

## Cruzamento de Identidade

1. **Email** — campo principal. Normalizado: lowercase + strip.
2. **Telefone** — secundário. Normalizado: apenas dígitos, sem DDI 55.
   - Se email bater → mesmo cliente (mesmo que telefone seja diferente)
   - Se email divergir mas telefone bater → mesmo cliente (sinalizado na tabela)

## Status Considerados (transações válidas)

- Hotmart: `APPROVED`, `COMPLETE`
- Guru: `paid`, `approved`, `complete`, `active`, `confirmed` (e sem status = incluir)

## Paginação

- Hotmart: cursor `page_token` (campo `page_info.next_page_token`)
- Guru: `page` + `per_page` (loop até resposta com menos itens que per_page ou last_page atingido)

## Endpoints Confirmados

### Hotmart
- Auth: `POST https://api-sec-vlc.hotmart.com/security/oauth/token`
- Vendas: `GET https://developers.hotmart.com/payments/api/v1/sales/history`
- Campos: `buyer.email`, `buyer.name`, `purchase.approved_date` (ms)

### Guru (Digital Manager Guru)
- Auth: Bearer token no header `Authorization`
- Transações: descoberto em runtime pelo script (self-annealing)
  - Candidatos: `/v2/transactions`, `/v1/transactions`, `/transactions`
- Assinaturas: `/subscriptions` (se disponível)
- Campos: `contact.email`, `contact.phone_number`, `contact.name`, `dates.ordered_at`

> **Atualizar esta seção** com o endpoint exato confirmado após primeira execução bem-sucedida.

## Dependências

```
pip install python-dotenv requests pandas streamlit
```

## Edge Cases

- **Mesmo cliente, emails diferentes**: não detectado automaticamente. Apenas email E telefone são cruzados.
- **Telefone sem DDI**: o script remove o prefixo 55 automaticamente para padronizar.
- **Compras sem email e sem telefone**: descartadas (não identificáveis).
- **Rate limiting**: ambos os scripts aguardam automaticamente (30s) se receberem status 429.
- **Token Hotmart expira**: válido por ~1h. O script obtém token novo a cada execução.

## Atualização dos Dados

Para atualizar com vendas mais recentes, basta re-executar os 3 primeiros scripts.
O dashboard recarrega automaticamente os CSVs (cache de 5 min).
