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

# Promotion codes de CRM (aquisição e reativação), iguais aos da aba Pacing. Comparação sem diferenciar maiúsculas.
CUPONS_CRM = [
    # Manual · aquisição
    "BR_CRM_40_OFF_ACQ",
    "BR_CRM_35_OFF_ACQ",
    "BR_CRM_30_OFF_ACQ",
    "BR_CRM_45_OFF_ACQ",
    "BR_PAYDAY_MAY_40_OFF_ACQ",
    # Manual · reativação (winback / churn)
    "40AGORA",
    "BR_CRM_20OFF_WINBACK",
    "MANUAL_BR_CRM_45OFF_CHURN",
    "BR_CRM_40_OFF_WINBACK",
    "BR_PAYDAY_30_OFF_CHURN",
    "BR_PAYDAY_35_OFF_CHURN",
    "BR_PAYDAY_MAY_40_OFF_CHURN",
    # Voy
    "voy_br_25_off_crm_acq",
    "voy_br_30_off_crm_acq",
    "voy_br_35_off_crm_acq",
    "VOY_CRM_POPUP_40_OFF",
    "voy_br_15_off_voyfriday_acq",
    "VOY_BR_20_OFF_CRM_ACQ_ALWAYSON_CPS",
    "VOY_BR_10_OFF_CRM_ACQ_ALWAYSON_CPS",
    "VOY_BR_25_OFF_CRM_ACQ_ALWAYSON_CPS",
    "VOY_BR_30_OFF_CRM_ACQ_ALWAYSON_CPS",
    "VOY_BR_CRM_20OFF_WINBACK",
]
CUPONS_CRM_SQL = ", ".join(f"'{c.lower()}'" for c in CUPONS_CRM)

# Quantos dias antes do checkout um envio ainda pode receber a atribuição.
JANELA_TOQUES_DIAS = 30


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
    # ------------------------------------------------------------------ tráfego
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
        # Desempenho por campanha e dia de entrada na jornada (horário de Brasília), últimos 12 meses.
        # Reprocessa 30 dias a cada execução porque engajamento e conversão chegam depois do envio.
        "name": "cio_campaigns_daily",
        "primary_key": "unique_key",
        "incremental_column": "DATE(j.campaign_journey_started_at, 'America/Sao_Paulo')",
        "incremental_por_data": True,
        "lookback_days": 30,
        "sql": f"""
SELECT
  TO_HEX(MD5(CONCAT(
    CAST(DATE(j.campaign_journey_started_at, 'America/Sao_Paulo') AS STRING), '|',
    IFNULL(j.cio_campaign_id, ''), '|', IFNULL(j.brand, '')
  ))) AS unique_key,
  DATE(j.campaign_journey_started_at, 'America/Sao_Paulo') AS journey_date,
  j.cio_campaign_id,
  j.brand,
  ANY_VALUE(j.campaign_name) AS campaign_name,
  ANY_VALUE(j.campaign_category) AS campaign_category,
  ANY_VALUE(j.campaign_taxonomy) AS campaign_taxonomy,
  ANY_VALUE(j.campaign_purpose) AS campaign_purpose,
  ANY_VALUE(j.campaign_group) AS campaign_group,
  COUNT(*) AS journeys,
  COUNT(DISTINCT j.customer_id) AS customers,
  COUNTIF(j.has_deliveries) AS journeys_delivered,
  COUNTIF(j.is_engaged) AS journeys_engaged,
  COUNTIF(j.is_converted) AS journeys_converted,
  COUNT(DISTINCT j.first_conversion_order_id) AS orders_converted
FROM `{LAKE}.dim_customerio_campaign_journeys` AS j
WHERE {brasil("j.country")}
  AND j.campaign_journey_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 366 DAY)
  AND DATE(j.campaign_journey_started_at, 'America/Sao_Paulo') >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 365 DAY)
""" + FILTRO + """
GROUP BY 1, 2, 3, 4
""",
    },
    {
        # Funil de e-mail por campanha, ACTION e dia de envio (horário de Brasília), últimos 12 meses.
        # Base para envios, entregas, aberturas e cliques por campanha/action na dash de CRM.
        # Reprocessa 30 dias porque aberturas e cliques chegam depois do envio.
        # versao = 3: granularidade por action e TIPO DE MENSAGEM (e-mail, push, sms, whatsapp...).
        # Linhas de versões anteriores são ignoradas na Nekt.
        "name": "cio_email_daily",
        "primary_key": "unique_key",
        "incremental_column": "DATE(COALESCE(d.sent_at, d.delivery_created_at), 'America/Sao_Paulo')",
        "incremental_por_data": True,
        "lookback_days": 30,
        "sql": f"""
SELECT
  TO_HEX(MD5(CONCAT(
    'v3|',
    CAST(DATE(COALESCE(d.sent_at, d.delivery_created_at), 'America/Sao_Paulo') AS STRING), '|',
    IFNULL(d.brand, ''), '|',
    IFNULL(d.cio_campaign_id, ''), '|', IFNULL(d.cio_newsletter_id, ''), '|', IFNULL(d.cio_action_id, ''), '|',
    IFNULL(LOWER(d.delivery_type), '')
  ))) AS unique_key,
  3 AS versao,
  DATE(COALESCE(d.sent_at, d.delivery_created_at), 'America/Sao_Paulo') AS send_date,
  d.brand,
  d.cio_campaign_id,
  d.cio_newsletter_id,
  d.cio_action_id,
  LOWER(d.delivery_type) AS delivery_type,
  ANY_VALUE(COALESCE(d.campaign_name, d.newsletter_name)) AS campaign_name,
  ANY_VALUE(d.action_name) AS action_name,
  ANY_VALUE(d.campaign_purpose) AS campaign_purpose,
  ANY_VALUE(d.campaign_group) AS campaign_group,
  ANY_VALUE(d.campaign_taxonomy) AS campaign_taxonomy,
  COUNT(*) AS deliveries,
  COUNTIF(d.is_sent) AS sent,
  COUNTIF(d.is_delivered) AS delivered,
  COUNTIF(d.is_opened) AS opened,
  COUNTIF(d.is_clicked) AS clicked,
  COUNTIF(d.is_converted) AS converted_cio,
  COUNTIF(d.is_unsubscribed) AS unsubscribed,
  COUNT(DISTINCT d.customer_id) AS customers,
  COUNT(DISTINCT IF(d.is_opened, d.customer_id, NULL)) AS customers_opened
FROM `{LAKE}.dim_customerio_deliveries` AS d
WHERE {brasil("d.country")}
  AND LOWER(COALESCE(d.delivery_type, '')) NOT IN ('webhook', 'slack', '')
  AND d.cio_transactional_message_id IS NULL
  AND COALESCE(d.sent_at, d.delivery_created_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 366 DAY)
""" + FILTRO + """
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
""",
    },
    {
        # Resumo de toques de e-mail por PEDIDO DE AQUISIÇÃO (com ou sem cupom), últimos 12 meses.
        # Uma linha por pedido (acquisition e unconfirmed-acquisition): último clique, última abertura
        # e último envio de e-mail da MESMA marca nos JANELA_TOQUES_DIAS antes do checkout, com campanha
        # e action de cada um. A regra de atribuição e a janela são aplicadas na Nekt e na dash.
        # Incremental pelo checkout: pedidos dos últimos 3 dias são (re)enviados.
        # versao = 3: uma linha por pedido e TIPO DE MENSAGEM (e-mail, push, sms, whatsapp...), com o último
        # clique, abertura e envio daquele tipo. Pedido sem nenhum toque vem numa linha com tipo vazio.
        # Linhas de versões anteriores são ignoradas na Nekt.
        "name": "cio_crm_order_touches",
        "primary_key": "unique_key",
        "incremental_column": "o.system_checkout_at",
        "lookback_days": 3,
        "sql": f"""
WITH descontos AS (
  -- Promotion code e cupom pelo DESCONTO APLICADO NOS ITENS do pedido (mesma fonte da aba Pacing).
  -- Se o pedido tiver mais de um código, fica o de CRM; empate, o primeiro em ordem alfabética.
  SELECT
    CAST(ip.order_id AS STRING) AS order_id,
    ARRAY_AGG(STRUCT(dd.promotion_code AS promotion_code, dd.promotion_coupon_code AS promotion_coupon_code)
              ORDER BY IF(LOWER(dd.promotion_code) IN ({CUPONS_CRM_SQL}), 0, 1), dd.promotion_code
              LIMIT 1)[OFFSET(0)] AS d
  FROM `{LAKE}.dim_order_item_parents` AS ip
  JOIN `{LAKE}.dim_order_item_discounts` AS dd ON dd.order_item_id = ip.order_item_id
  WHERE dd.promotion_code IS NOT NULL
  GROUP BY 1
),
clientes AS (
  -- Id do cliente no Customer.io (igual ao dim_customer_cio_customer_id da aba AcquisitionsCRM / Pacing)
  SELECT CAST(customer_id AS STRING) AS customer_id, ANY_VALUE(cio_customer_id) AS cio_customer_id
  FROM `{LAKE}.dim_customer`
  GROUP BY 1
),
pedidos AS (
  SELECT
    CAST(o.order_id AS STRING) AS order_id,
    CAST(o.order_number AS STRING) AS order_number,
    CAST(o.customer_id AS STRING) AS customer_id,
    IF(o.brand_at_checkout = 'Manual', 'MANUAL', o.brand_at_checkout) AS brand,
    CAST(o.system_checkout_at AS TIMESTAMP) AS checkout_at,
    CAST(o.payment_completed_at AS TIMESTAMP) AS completed_at,
    o.order_context,
    o.is_confirmed,
    o.is_acquisition_order,
    ds.d.promotion_code AS promotion_code,
    ds.d.promotion_coupon_code AS promotion_coupon_code,
    cl.cio_customer_id AS cio_customer_id
  FROM `{LAKE}.fct_orders` AS o
  LEFT JOIN descontos AS ds ON ds.order_id = CAST(o.order_id AS STRING)
  LEFT JOIN clientes AS cl ON cl.customer_id = CAST(o.customer_id AS STRING)
  WHERE {brasil("o.country")}
    -- Só aquisição e reativação (pedidos recorrentes e incrementais ficam de fora)
    AND o.order_context IN ('acquisition', 'unconfirmed-acquisition', 'reactivation', 'unconfirmed-reactivation')
    AND LOWER(COALESCE(o.order_taxonomy_business_category, '')) <> 'ed'
    AND NOT COALESCE(o.is_internal_user, FALSE)
    AND CAST(o.system_checkout_at AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
    """ + FILTRO + f"""
),
toques AS (
  SELECT
    pc.order_id,
    LOWER(d.delivery_type) AS tipo,
    STRUCT(d.cio_campaign_id AS campaign_id, COALESCE(d.campaign_name, d.newsletter_name) AS campaign,
           d.cio_action_id AS action_id, d.action_name AS action) AS m,
    COALESCE(d.delivered_at, d.sent_at) AS entregue_at,
    IF(d.opened_at < pc.checkout_at, d.opened_at, NULL) AS aberto_at,
    IF(d.clicked_at < pc.checkout_at, d.clicked_at, NULL) AS clicado_at
  FROM pedidos AS pc
  JOIN `{LAKE}.dim_customerio_deliveries` AS d
    ON CAST(d.customer_id AS STRING) = pc.customer_id
   AND d.brand = pc.brand
  WHERE {brasil("d.country")}
    AND LOWER(COALESCE(d.delivery_type, '')) NOT IN ('webhook', 'slack', '')
    AND d.cio_transactional_message_id IS NULL
    AND COALESCE(d.delivered_at, d.sent_at) < pc.checkout_at
    AND COALESCE(d.delivered_at, d.sent_at) >= TIMESTAMP_SUB(pc.checkout_at, INTERVAL {JANELA_TOQUES_DIAS} DAY)
    {{{{FILTRO_TOQUES}}}}
),
resumo AS (
  SELECT
    order_id,
    tipo,
    ARRAY_AGG(IF(clicado_at IS NULL, NULL, STRUCT(m, clicado_at AS ts)) IGNORE NULLS ORDER BY clicado_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS clique,
    ARRAY_AGG(IF(aberto_at IS NULL, NULL, STRUCT(m, aberto_at AS ts)) IGNORE NULLS ORDER BY aberto_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS abertura,
    ARRAY_AGG(STRUCT(m, entregue_at AS ts) ORDER BY entregue_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS envio,
    COUNT(*) AS emails_entregues,
    COUNT(aberto_at) AS emails_abertos,
    COUNT(clicado_at) AS emails_clicados
  FROM toques
  GROUP BY 1, 2
)
SELECT
  TO_HEX(MD5(CONCAT('v3|', pc.order_id, '|', IFNULL(r.tipo, '')))) AS unique_key,
  3 AS versao,
  r.tipo AS delivery_type,
  pc.order_id, pc.order_number, pc.customer_id, pc.brand,
  pc.checkout_at, pc.completed_at, pc.order_context, pc.is_confirmed, pc.is_acquisition_order,
  pc.promotion_code, pc.promotion_coupon_code, pc.cio_customer_id,
  r.clique.ts AS clique_at, r.clique.m.campaign_id AS clique_campaign_id, r.clique.m.campaign AS clique_campaign,
  r.clique.m.action_id AS clique_action_id, r.clique.m.action AS clique_action,
  r.abertura.ts AS abertura_at, r.abertura.m.campaign_id AS abertura_campaign_id, r.abertura.m.campaign AS abertura_campaign,
  r.abertura.m.action_id AS abertura_action_id, r.abertura.m.action AS abertura_action,
  r.envio.ts AS envio_at, r.envio.m.campaign_id AS envio_campaign_id, r.envio.m.campaign AS envio_campaign,
  r.envio.m.action_id AS envio_action_id, r.envio.m.action AS envio_action,
  COALESCE(r.emails_entregues, 0) AS emails_entregues,
  COALESCE(r.emails_abertos, 0) AS emails_abertos,
  COALESCE(r.emails_clicados, 0) AS emails_clicados
FROM pedidos AS pc
LEFT JOIN resumo AS r USING (order_id)
""",
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
    # Filtro extra para tabelas de toques (dim_customerio_deliveries): limita a leitura de envios
    # ao período que pode tocar os pedidos desta execução (reduz o volume lido no BigQuery).
    janela = int(JANELA_TOQUES_DIAS) + 1
    if e_full(table):
        filtro_toques = (f"AND COALESCE(d.delivered_at, d.sent_at) >= "
                         f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {365 + janela} DAY)")
    else:
        dias = int(table.get("lookback_days", 3)) + janela
        filtro_toques = (f"AND COALESCE(d.delivered_at, d.sent_at) >= "
                         f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {dias} DAY)")
    return (table["sql"]
            .replace("{{FILTRO_INCREMENTAL}}", filtro)
            .replace("{{FILTRO_TOQUES}}", filtro_toques))


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
