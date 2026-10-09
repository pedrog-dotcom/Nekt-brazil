"""
Lake (BigQuery) -> Nekt via Webhook, em lotes.

- Lê cada tabela configurada em TABLES direto do BigQuery, página por página (nada fica inteiro em memória).
- Envia os registros em lotes para o Webhook da Nekt no formato {"records": [...]}.
  Na source Webhook da Nekt: "Use payload schema template" = true, JSONPath = $.records[*]
  e Primary keys = a chave da tabela (assim reenvios viram upsert, sem duplicar).
- Modo incremental: só as linhas dos últimos N dias da coluna incremental (janela de segurança).
- Modo full refresh (FULL_REFRESH=true): reenvia a tabela inteira; o upsert por chave mantém a tabela correta.

Uso:
  python sync_lake_nekt.py              -> todas as tabelas
  python sync_lake_nekt.py customers    -> só as tabelas citadas

Variáveis de ambiente:
  GOOGLE_APPLICATION_CREDENTIALS  caminho do JSON de credencial (ver README da conversa)
  BQ_BILLING_PROJECT              projeto de faturamento das queries
  NEKT_WEBHOOK_<TABELA>           URL do webhook de cada tabela (ex.: NEKT_WEBHOOK_CUSTOMERS)
  NEKT_WEBHOOK_API_KEY            opcional, valor da API key configurada nos webhooks
  NEKT_API_KEY_HEADER             opcional, nome do header (padrão x-api-key)
  FULL_REFRESH                    "true" para ignorar o filtro incremental
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

PAGE_SIZE = 10_000            # linhas por página lida do BigQuery
MAX_BATCH_BYTES = 2_000_000   # tamanho máximo de cada POST (~2 MB)
MAX_BATCH_RECORDS = 2_000     # registros máximos por POST
MAX_RETRIES = 6

# ---------------------------------------------------------------------------
# Tabelas. Para adicionar outra, copie um bloco e crie o secret NEKT_WEBHOOK_<NOME>.
# Coloque {{FILTRO_INCREMENTAL}} dentro do WHERE da SQL; o script troca pelo filtro
# de data no modo incremental e remove no full refresh.
# ---------------------------------------------------------------------------
TABLES = [
    {
        "name": "customers",
        "webhook_env": "NEKT_WEBHOOK_CUSTOMERS",
        # Coluna usada no filtro incremental, qualificada como aparece na SQL.
        # Se a tabela tiver updated_at, prefira ela: assim alterações também entram no incremental.
        "incremental_column": "dim_customer.created_at",
        "lookback_days": 3,
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
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', CAST(dim_customer.created_at AS TIMESTAMP)) AS dim_customer_created_at
FROM `manual-data-team.prod.dim_customer` AS dim_customer
LEFT JOIN `manual-data-team.prod.fct_customer_activity_daily` AS fct_customer_activity_daily
  ON dim_customer.customer_id = fct_customer_activity_daily.customer_id
  AND fct_customer_activity_daily.is_current_activity
LEFT JOIN `manual-data-team.prod.dim_customer` AS referred_by
  ON dim_customer.referred_by_customer_id = referred_by.customer_id
WHERE INITCAP(REPLACE(dim_customer.country, "_", " ")) = 'Brazil'
  {{FILTRO_INCREMENTAL}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
""",
    },
    # {
    #     "name": "orders",
    #     "webhook_env": "NEKT_WEBHOOK_ORDERS",
    #     "incremental_column": "o.updated_at",
    #     "lookback_days": 3,
    #     "sql": "SELECT ... FROM `manual-data-team.prod.fct_orders` AS o WHERE TRUE {{FILTRO_INCREMENTAL}}",
    # },
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


def montar_sql(table):
    col = table.get("incremental_column")
    if FULL_REFRESH or not col:
        filtro = ""
    else:
        dias = int(table.get("lookback_days", 3))
        filtro = (f"AND CAST({col} AS TIMESTAMP) >= "
                  f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {dias} DAY)")
    return table["sql"].replace("{{FILTRO_INCREMENTAL}}", filtro)


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


def sync_tabela(client, session, table):
    nome = table["name"]
    url = os.environ.get(table["webhook_env"])
    if not url:
        raise RuntimeError(f"Variável {table['webhook_env']} não definida.")

    modo = "full refresh" if (FULL_REFRESH or not table.get("incremental_column")) else \
        f"incremental ({table.get('lookback_days', 3)} dias)"
    log(f"{nome}: iniciando {modo}")

    job = client.query(montar_sql(table))
    linhas = job.result(page_size=PAGE_SIZE)
    log(f"{nome}: query pronta, {linhas.total_rows} linhas para enviar")

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

    log(f"{nome}: concluído, {total} linhas em {lotes} lotes")


def main():
    filtro = set(sys.argv[1:])
    tabelas = [t for t in TABLES if not filtro or t["name"] in filtro]
    if not tabelas:
        sys.exit(f"Nenhuma tabela encontrada para: {', '.join(filtro)}")

    client = bigquery.Client(project=BILLING_PROJECT)
    session = requests.Session()

    falhas = []
    for t in tabelas:
        try:
            sync_tabela(client, session, t)
        except Exception as e:  # uma tabela com erro não derruba as outras
            log(f"{t['name']}: ERRO {e}")
            falhas.append(t["name"])

    if falhas:
        sys.exit(f"Tabelas com erro: {', '.join(falhas)}")
    log("Tudo certo.")


if __name__ == "__main__":
    main()
