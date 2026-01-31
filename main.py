import os
import time
import requests
import pandas as pd
import schedule
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import json


# --- CONFIGURAÇÃO DE LOGS (HORA BRASIL - SEM PYTZ) ---
# Isso resolve o problema da hora mostrando 3h a mais
class BrazilFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, timezone.utc) - timedelta(hours=3)
        return dt.strftime(datefmt if datefmt else "%Y-%m-%d %H:%M:%S")


handler = logging.StreamHandler()
handler.setFormatter(BrazilFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Limpa handlers antigos para não duplicar logs
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(handler)

# --- CONFIGURAÇÕES DE AMBIENTE ---
DB_HOST = os.getenv("DB_HOST", "patroni_primary")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "relatorio_meta_ads")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID_LIST = os.getenv("AD_ACCOUNTS", "").split(",")

API_VERSION = "v24.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# Conexão Banco
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def get_date_range():
    today = datetime.today()
    first = today.replace(day=1)
    previous_month = (first - timedelta(days=1)).replace(day=1)
    since = previous_month.strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")
    return since, until


def clear_existing_data(account_id, since, until):
    """NOVIDADE: Apaga dados antigos ANTES de começar, para evitar duplicidade"""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM insights_meta_ads WHERE account_id = :acc AND data_registro >= :s AND data_registro <= :u"
                ),
                {"acc": account_id, "s": since, "u": until},
            )
        logger.info(f"🧹 Limpeza prévia realizada para a conta {account_id}")
    except Exception as e:
        logger.error(f"Erro ao limpar dados: {e}")


def transform_and_load(raw_data_page, account_id):
    """NOVIDADE: Processa e salva IMEDIATAMENTE (Streaming)"""
    if not raw_data_page:
        return

    df = pd.DataFrame(raw_data_page)
    df["account_id"] = account_id
    df["nome_conta"] = f"Conta {account_id}"

    def process_actions(row, action_type_target):
        if isinstance(row, list):
            for item in row:
                if item.get("action_type") == action_type_target:
                    return float(item.get("value", 0))
        return 0.0

    # Tratamento numérico seguro
    for col in ["impressions", "spend", "inline_link_clicks"]:
        df[col] = pd.to_numeric(df.get(col, 0)).fillna(0)

    # Mapeamento de Actions
    if "actions" in df.columns:
        mapping = {
            "lp_view": "landing_page_view",
            "lead": "lead",
            "contato": "contact",
            "conversas_iniciadas": "onsite_conversion.messaging_conversation_started_7d",
            "novos_contatos_mensagem": "onsite_conversion.messaging_first_reply",
            "seguidores_instagram": "follow",
            "visitas_perfil": "profile_visit",
            "initiate_checkout": "initiate_checkout",
            "compras": "purchase",
            "cliques_saida": "outbound_click",
            "videoview_3s": "video_view",
        }
        for col, key in mapping.items():
            df[col] = df["actions"].apply(lambda x: process_actions(x, key))
    else:
        cols = [
            "lp_view",
            "lead",
            "contato",
            "conversas_iniciadas",
            "novos_contatos_mensagem",
            "seguidores_instagram",
            "visitas_perfil",
            "initiate_checkout",
            "compras",
            "cliques_saida",
            "videoview_3s",
        ]
        for c in cols:
            df[c] = 0.0

    df["valor_compra"] = 0.0
    df["videoview_50"] = 0.0
    df["videoview_75"] = 0.0

    df.rename(
        columns={
            "campaign_id": "id_campanha",
            "campaign_name": "campanha",
            "adset_id": "id_conjunto_anuncios",
            "adset_name": "conjunto_anuncios",
            "ad_id": "id_anuncio",
            "ad_name": "anuncio",
            "date_start": "data_registro",
            "publisher_platform": "plataforma",
            "platform_position": "posicionamento",
            "spend": "valor_gasto",
            "impressions": "impressoes",
            "inline_link_clicks": "clique_link",
        },
        inplace=True,
    )

    final_cols = [
        "account_id",
        "nome_conta",
        "id_campanha",
        "id_conjunto_anuncios",
        "id_anuncio",
        "campanha",
        "conjunto_anuncios",
        "anuncio",
        "impressoes",
        "cliques_saida",
        "clique_link",
        "lp_view",
        "lead",
        "contato",
        "conversas_iniciadas",
        "novos_contatos_mensagem",
        "seguidores_instagram",
        "visitas_perfil",
        "initiate_checkout",
        "compras",
        "valor_compra",
        "data_registro",
        "videoview_3s",
        "videoview_50",
        "videoview_75",
        "plataforma",
        "posicionamento",
        "valor_gasto",
    ]

    for col in final_cols:
        if col not in df.columns:
            df[col] = 0

    try:
        with engine.begin() as conn:
            # Chunksize menor para garantir que passa no cano
            df[final_cols].to_sql(
                "insights_meta_ads",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )
    except Exception as e:
        logger.error(f"Erro ao salvar no banco: {e}")


def fetch_and_process(account_id, since, until):
    clean_id = account_id.strip()
    if not clean_id.startswith("act_"):
        clean_id = f"act_{clean_id}"

    # 1. Limpa TUDO antes de começar
    clear_existing_data(clean_id, since, until)

    url = f"{BASE_URL}/{clean_id}/insights"
    fields = [
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "impressions",
        "spend",
        "inline_link_clicks",
        "actions",
    ]

    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "fields": ",".join(fields),
        "breakdowns": "publisher_platform,platform_position",
        "limit": 50,
    }

    page = 0
    total = 0

    while True:
        try:
            page += 1
            response = requests.get(url, params=params, timeout=60)

            if response.status_code != 200:
                logger.error(f"❌ Erro API ({clean_id}): {response.text}")
                break

            data = response.json()

            if "data" in data and len(data["data"]) > 0:
                # 2. Salva AGORA (não guarda na memória)
                transform_and_load(data["data"], clean_id)
                count = len(data["data"])
                total += count
                logger.info(
                    f"   💾 Pág {page} salva (+{count} regs) | Total conta: {total}"
                )

            if "x-fb-ads-insights-throttle" in response.headers:
                try:
                    throttle = json.loads(
                        response.headers["x-fb-ads-insights-throttle"]
                    )
                    if throttle.get("acc_id_util_pct", 0) > 90:
                        logger.warning("⚠️ Rate limit alto. Pausa de 3 min.")
                        time.sleep(180)
                except:
                    pass

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
                params = {}
            else:
                logger.info(f"🏁 Conta {clean_id} finalizada. Total: {total}")
                break

        except Exception as e:
            logger.error(f"❌ Erro fatal pág {page}: {e}")
            break


def run_etl():
    logger.info("🚀 INICIANDO ETL (Modo Otimizado - Baixa RAM)")
    since, until = get_date_range()

    # Tratamento seguro para lista vazia
    raw_accounts = os.getenv("AD_ACCOUNTS", "")
    accounts = [acc.strip() for acc in raw_accounts.split(",") if acc.strip()]

    if not accounts:
        logger.warning("⚠️ Nenhuma conta configurada em AD_ACCOUNTS!")
        return

    for account_id in accounts:
        logger.info(f"--- Iniciando {account_id} ---")
        fetch_and_process(account_id, since, until)

    logger.info("✅ JOB FINALIZADO - Próxima execução em 4h")


# Loop
run_etl()
schedule.every(4).hours.do(run_etl)
while True:
    schedule.run_pending()
    time.sleep(60)
