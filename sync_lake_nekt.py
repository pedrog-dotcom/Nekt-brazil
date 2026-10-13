"""
Lake (BigQuery) -> Nekt via Webhook, em lotes.

- Lê cada tabela configurada em TABLES direto do BigQuery, página por página (nada fica inteiro em memória).
- Envia os registros em lotes para o Webhook da Nekt no formato {"records": [...]}.
  Em cada source Webhook da Nekt: "Use payload schema template" = true, JSONPath = $.records[*]
  e Primary keys = a chave da tabela (assim reenvios viram upsert, sem duplicar).
- Modo incremental: só as linhas dos últimos N dias da coluna incremental (janela de segurança).
- Modo full refresh (FULL_REFRESH=true): reenvia a tabela inteira (respeitando a janela fixa da SQL, se houver).
- Tabelas sem incremental_column são sempre enviadas inteiras (são pequenas).

Uso:
  python sync_lake_nekt.py                      -> todas as tabelas
  python sync_lake_nekt.py orders customers     -> só as tabelas citadas

Variáveis de ambiente:
  GOOGLE_APPLICATION_CREDENTIALS  caminho do JSON de credencial do Google
  BQ_BILLING_PROJECT              projeto de faturamento das queries
  NEKT_WEBHOOKS_JSON              JSON {"nome_da_tabela": "url do webhook", ...} com todas as URLs
  NEKT_WEBHOOK_<NOME>             alternativa por tabela (ex.: NEKT_WEBHOOK_CUSTOMERS); o JSON tem prioridade
  NEKT_WEBHOOK_API_KEY            opcional, valor da API key configurada nos webhooks
  NEKT_API_KEY_HEADER             opcional, nome do header (padrão x-api-key)
  FULL_REFRESH                    "true" para ignorar o filtro incremental
  FULL_REFRESH_ORIGEM             "schedule" (full semanal automático) ou "manual" (padrão)
"""

import base64
import datetime as dt
import decimal
import json
import os
import sys
import time

import requests
from google.cloud import bigquery

BILLING_PROJECT = os.environ.get("BQ_BILLING_PROJECT", "enduring-coda-490721-k1")
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_ORIGEM = os.environ.get("FULL_REFRESH_ORIGEM", "manual").lower()

PAGE_SIZE = 10_000            # linhas por página lida do BigQuery
MAX_BATCH_BYTES = 2_000_000   # tamanho máximo de cada POST (~2 MB)
MAX_BATCH_RECORDS = 2_000     # registros máximos por POST
MAX_RETRIES = 6

LAKE = "manual-data-team.prod"


# Trecho colocado no fim do WHERE de cada SQL; o script troca pelo filtro de data.
FILTRO = """  {{FILTRO_INCREMENTAL}}
"""


def brasil(coluna):
    """Filtro de Brasil tolerante a formatos diferentes (brazil, BRAZIL, BR, BRA)."""
    return f"LOWER(REPLACE({coluna}, '_', ' ')) IN ('brazil', 'br', 'bra')"


# ---------------------------------------------------------------------------
# Tabelas. Para adicionar outra, copie um bloco, crie a source Webhook na Nekt
# e inclua a URL no secret NEKT_WEBHOOKS_JSON com o mesmo "name".
#
# Campos:
#   name                 nome da tabela no script e chave no NEKT_WEBHOOKS_JSON
#   primary_key          chave configurada na source da Nekt (só documentação)
#   incremental_column   coluna (ou expressão) do filtro incremental; None = sempre inteira
#   incremental_por_data True quando a coluna é uma DATA e o filtro deve pegar dias inteiros
#   lookback_days        janela do incremental
#   full_semanal         False = o full refresh automático de domingo não reenvia essa tabela
#                        (o full refresh manual reenvia todas)
#   sql                  query; {{FILTRO_INCREMENTAL}} é trocado pelo filtro de data
# ---------------------------------------------------------------------------
TABLES = [
    # ----------------------------------------------------------------- clientes
    {
        "name": "customers",
        "primary_key": "dim_customer_customer_id",
        # Sempre inteira: referral_count e is_active mudam em clientes antigos.
        "incremental_column": None,
        "sql": """
SELECT
  dim_customer.channel_code AS dim_customer_channel_code,
  IF(dim_customer.brand = 'Manual', 'MANUAL', dim_customer.brand) AS dim_customer_brand,
  dim_customer.optimale_salesforce_account_id AS dim_customer_optimale_salesforce_account_id,
  dim_customer.company AS dim_customer_company,
  dim_customer.patient_id AS dim_customer_patient_id,
  INITCAP(REPLACE(dim_customer.country, "_", " ")) AS dim_customer_country,
  dim_customer.customer_id AS dim_customer_customer_id,
  (CASE WHEN fct_customer_activity_daily.is_active THEN 'Yes' ELSE 'No' END) AS fct_customer_activity_daily_is_active,
  dim_customer.postcode AS dim_customer_postcode,
  (CASE WHEN dim_customer.referral_count > 0 THEN 'Yes' ELSE 'No' END) AS dim_customer_is_inviter,
  dim_customer.referred_by_customer_id AS dim_customer_referred_by_customer_id,
  dim_customer.referral_count AS dim_customer_referral_count,
  DATE_DIFF(DATE(dim_customer.created_at), DATE(referred_by.created_at), DAY) AS dim_customer_days_to_get_referred,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', CAST(dim_customer.created_at AS TIMESTAMP)) AS dim_customer_created_at,
  dim_customer.approved_refer_rank AS dim_customer_approved_refer_rank
FROM `manual-data-team.prod.dim_customer` AS dim_customer
LEFT JOIN `manual-data-team.prod.fct_customer_activity_daily` AS fct_customer_activity_daily
  ON dim_customer.customer_id = fct_customer_activity_daily.customer_id
  AND fct_customer_activity_daily.is_current_activity
LEFT JOIN `manual-data-team.prod.dim_customer` AS referred_by
  ON dim_customer.referred_by_customer_id = referred_by.customer_id
WHERE INITCAP(REPLACE(dim_customer.country, "_", " ")) = 'Brazil'
  {{FILTRO_INCREMENTAL}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
""",
    },
    {
        "name": "places_heard_from",
        "primary_key": "unique_key",
        "incremental_column": None,
        "sql": f"""
SELECT h.unique_key, h.customer_id, h.place_heard_from
FROM `{LAKE}.dim_customer_places_heard_from` AS h
JOIN `{LAKE}.dim_customer` AS c ON c.customer_id = h.customer_id
WHERE {brasil("c.country")}
""" + FILTRO,
    },
    # ------------------------------------------------------ pedidos e assinaturas
    {
        "name": "orders",
        "primary_key": "order_id",
        "incremental_column": "o.order_updated_at",
        "lookback_days": 3,
        "sql": f"""
SELECT
  o.* EXCEPT (clinic_booking_json, products_in_basket, order_tags),
  TO_JSON_STRING(o.products_in_basket) AS products_in_basket,
  TO_JSON_STRING(o.order_tags) AS order_tags
FROM `{LAKE}.fct_orders` AS o
WHERE {brasil("o.country")}
""" + FILTRO,
    },
    {
        "name": "subscriptions",
        "primary_key": "subscription_id",
        "incremental_column": "s.updated_at",
        "lookback_days": 3,
        "sql": f"""
SELECT
  s.* EXCEPT (current_dosage_recommendation, requested_change_jsons, subscription_tags,
              all_next_order_dates, all_next_order_and_renewal_dates),
  TO_JSON_STRING(s.subscription_tags) AS subscription_tags
FROM `{LAKE}.dim_subscription` AS s
WHERE {brasil("s.country")}
""" + FILTRO,
    },
    # ------------------------------------------------------------------- cupons
    {
        "name": "order_discounts",
        "primary_key": "unique_key",
        "incremental_column": "d.system_checkout_at",
        "lookback_days": 3,
        "sql": f"""
SELECT d.*
FROM `{LAKE}.fct_order_discounts` AS d
WHERE d.currency_code = 'BRL'
""" + FILTRO,
    },
    {
        "name": "promotions",
        "primary_key": "unique_key",
        "incremental_column": "COALESCE(p.promotion_coupon_updated_at, p.promotion_updated_at)",
        "lookback_days": 3,
        "sql": f"""
SELECT p.*
FROM `{LAKE}.dim_promotions` AS p
WHERE TRUE
""" + FILTRO,
    },
    {
        "name": "promotion_campaigns",
        "primary_key": "promotion_id",
        "incremental_column": None,
        "sql": f"""
SELECT pc.*
FROM `{LAKE}.dim_promotion_campaigns` AS pc
WHERE TRUE
""" + FILTRO,
    },
    {
        "name": "influencer_promo_codes",
        "primary_key": "unique_key",
        "incremental_column": None,
        "sql": f"""
SELECT ipc.* EXCEPT (influencer_email)
FROM `{LAKE}.dim_influencer_promo_codes` AS ipc
WHERE ipc.currency_code = 'BRL'
""" + FILTRO,
    },
    # ---------------------------------------------------------- tráfego e UTMs
    {
        "name": "utms",
        "primary_key": "unique_key",
        "incremental_column": "u.utm_last_seen_at",
        "lookback_days": 3,
        "sql": f"""
SELECT u.*
FROM `{LAKE}.dim_utms` AS u
WHERE {brasil("u.market")}
""" + FILTRO,
    },
    {
        # Sessões agregadas por dia (horário de Brasília) e fonte de tráfego, últimos 2 anos.
        "name": "sessions_by_source",
        "primary_key": "unique_key",
        "incremental_column": "DATE(s.session_started_at, 'America/Sao_Paulo')",
        "incremental_por_data": True,
        "lookback_days": 3,
        "sql": f"""
SELECT
  TO_HEX(MD5(CONCAT(
    CAST(DATE(s.session_started_at, 'America/Sao_Paulo') AS STRING), '|',
    IFNULL(s.brand, ''), '|', IFNULL(s.device, ''), '|', IFNULL(s.landing_category, ''), '|',
    IFNULL(s.attribution_channel, ''), '|', IFNULL(s.attribution_source, ''), '|',
    IFNULL(s.utm_source, ''), '|', IFNULL(s.utm_medium, ''), '|', IFNULL(s.utm_campaign, '')
  ))) AS unique_key,
  DATE(s.session_started_at, 'America/Sao_Paulo') AS session_date,
  s.brand,
  s.device,
  s.landing_category,
  s.attribution_channel,
  s.attribution_source,
  s.utm_source,
  s.utm_medium,
  s.utm_campaign,
  COUNT(*) AS sessions,
  COUNT(DISTINCT s.anonymous_id) AS visitors,
  COUNTIF(s.user_session_rank = 1) AS first_sessions,
  COUNTIF(s.order_number IS NOT NULL) AS sessions_with_order
FROM `{LAKE}.fct_web_sessions` AS s
WHERE {brasil("s.country")}
  AND s.session_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 731 DAY)
  AND DATE(s.session_started_at, 'America/Sao_Paulo') >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 730 DAY)
""" + FILTRO + """
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
""",
    },
    {
        # Sessão em que cada pedido foi feito, com canal e UTMs, últimos 2 anos.
        "name": "order_sessions",
        "primary_key": "gtm_session_unique_key",
        "incremental_column": "s.session_started_at",
        "lookback_days": 3,
        "sql": f"""
SELECT
  s.gtm_session_unique_key,
  o.order_id,
  s.order_number,
  s.session_started_at,
  s.brand,
  s.device,
  s.landing_category,
  s.landing_page,
  s.attribution_channel,
  s.attribution_source,
  s.referring_page,
  s.utm_source,
  s.utm_medium,
  s.utm_campaign,
  s.utm_content,
  s.user_session_rank
FROM `{LAKE}.fct_web_sessions` AS s
LEFT JOIN `{LAKE}.fct_orders` AS o
  ON o.order_number = s.order_number
WHERE {brasil("s.country")}
  AND s.order_number IS NOT NULL
  AND s.session_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY)
""" + FILTRO,
    },
    # --------------------------------------------------------------- Customer.io
    {
        # Uma linha por cliente por campanha, últimos 12 meses, sem e-mail.
        "name": "cio_campaign_journeys",
        "primary_key": "cio_campaign_journey_unique_key",
        "incremental_column": "j.campaign_journey_started_at",
        "lookback_days": 30,          # conversões acontecem dias depois do envio
        "full_semanal": False,        # tabela pesada: só reenvia inteira no full refresh manual
        "sql": f"""
SELECT j.* EXCEPT (email, customer_identifier)
FROM `{LAKE}.dim_customerio_campaign_journeys` AS j
WHERE {brasil("j.country")}
  AND j.campaign_journey_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
""" + FILTRO,
    },
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def json_default(v):
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return str(v)  # string para não perder precisão
    if isinstance(v, bytes):
        return base64.b64encode(v).decode()
    return str(v)


def e_full(table):
    """Decide se a tabela vai inteira nesta execução."""
    if not table.get("incremental_column"):
        return True
    if not FULL_REFRESH:
        return False
    if FULL_REFRESH_ORIGEM == "schedule" and table.get("full_semanal", True) is False:
        return False
    return True


def montar_sql(table):
    col = table.get("incremental_column")
    if e_full(table):
        filtro = ""
    else:
        dias = int(table.get("lookback_days", 3))
        if table.get("incremental_por_data"):
            filtro = (f"AND {col} >= "
                      f"DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL {dias} DAY)")
        else:
            filtro = (f"AND CAST({col} AS TIMESTAMP) >= "
                      f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {dias} DAY)")
    return table["sql"].replace("{{FILTRO_INCREMENTAL}}", filtro)


def carregar_webhooks():
    bruto = os.environ.get("NEKT_WEBHOOKS_JSON", "").strip()
    if not bruto:
        return {}
    try:
        dados = json.loads(bruto)
    except json.JSONDecodeError as e:
        sys.exit(f"NEKT_WEBHOOKS_JSON não é um JSON válido: {e}")
    if not isinstance(dados, dict):
        sys.exit('NEKT_WEBHOOKS_JSON precisa ser um objeto: {"tabela": "url", ...}')
    return {str(k).strip().lower(): str(v).strip() for k, v in dados.items()}


def url_webhook(table, webhooks):
    nome = table["name"].lower()
    url = webhooks.get(nome) or os.environ.get(f"NEKT_WEBHOOK_{nome.upper()}")
    if not url:
        raise RuntimeError(
            f"URL do webhook de '{nome}' não encontrada. Inclua '{nome}' no secret "
            f"NEKT_WEBHOOKS_JSON (ou crie NEKT_WEBHOOK_{nome.upper()})."
        )
    return url


def headers():
    h = {"Content-Type": "application/json"}
    key = os.environ.get("NEKT_WEBHOOK_API_KEY")
    if key:
        h[os.environ.get("NEKT_API_KEY_HEADER", "x-api-key")] = key
    return h


def enviar(session, url, partes):
    """partes = lista de registros já serializados em JSON."""
    body = ('{"records":[' + ",".join(partes) + "]}").encode("utf-8")
    ultimo_erro = None
    for tentativa in range(MAX_RETRIES):
        try:
            r = session.post(url, data=body, headers=headers(), timeout=90)
            if r.status_code < 300:
                return
            if r.status_code not in (408, 429) and r.status_code < 500:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")  # erro do nosso lado, não adianta repetir
            ultimo_erro = f"HTTP {r.status_code}: {r.text[:300]}"
        except requests.RequestException as e:
            ultimo_erro = repr(e)
        espera = min(60, 2 ** tentativa)
        log(f"  falha no envio ({ultimo_erro}); nova tentativa em {espera}s")
        time.sleep(espera)
    raise RuntimeError(f"Envio falhou após {MAX_RETRIES} tentativas: {ultimo_erro}")


def sync_tabela(client, session, table, webhooks):
    nome = table["name"]
    url = url_webhook(table, webhooks)
    inicio = time.time()

    if e_full(table):
        modo = "inteira" if not table.get("incremental_column") else "full refresh"
    else:
        modo = f"incremental ({table.get('lookback_days', 3)} dias)"
    log(f"{nome}: iniciando {modo}")

    job = client.query(montar_sql(table))
    linhas = job.result(page_size=PAGE_SIZE)
    gb = (job.total_bytes_processed or 0) / 1024 ** 3
    log(f"{nome}: query pronta, {linhas.total_rows} linhas para enviar ({gb:.2f} GB processados)")

    lote, lote_bytes, total, lotes = [], 0, 0, 0
    for row in linhas:  # o iterador busca página por página
        parte = json.dumps(dict(row.items()), default=json_default, ensure_ascii=False)
        tamanho = len(parte.encode("utf-8")) + 1
        if lote and (lote_bytes + tamanho > MAX_BATCH_BYTES or len(lote) >= MAX_BATCH_RECORDS):
            enviar(session, url, lote)
            total += len(lote)
            lotes += 1
            if lotes % 50 == 0:
                log(f"{nome}: {total} linhas enviadas")
            lote, lote_bytes = [], 0
        lote.append(parte)
        lote_bytes += tamanho

    if lote:
        enviar(session, url, lote)
        total += len(lote)
        lotes += 1

    log(f"{nome}: concluído, {total} linhas em {lotes} lotes ({time.time() - inicio:.0f}s)")


def main():
    filtro = {a.strip().lower() for a in sys.argv[1:] if a.strip()}
    tabelas = [t for t in TABLES if not filtro or t["name"] in filtro]
    desconhecidas = filtro - {t["name"] for t in TABLES}
    if desconhecidas:
        log(f"Aviso: tabelas não configuradas e ignoradas: {', '.join(sorted(desconhecidas))}")
    if not tabelas:
        sys.exit(f"Nenhuma tabela encontrada para: {', '.join(sorted(filtro))}")

    webhooks = carregar_webhooks()
    client = bigquery.Client(project=BILLING_PROJECT)
    session = requests.Session()

    falhas = []
    for t in tabelas:
        try:
            sync_tabela(client, session, t, webhooks)
        except Exception as e:  # uma tabela com erro não derruba as outras
            log(f"{t['name']}: ERRO {e}")
            falhas.append(t["name"])

    if falhas:
        sys.exit(f"Tabelas com erro: {', '.join(falhas)}")
    log("Tudo certo.")


if __name__ == "__main__":
    main()
