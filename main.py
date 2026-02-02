import os
import time
import requests
import pandas as pd
import schedule
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import json


# --- CONFIGURAÇÃO DE LOGS (HORA BRASIL) ---
class BrazilFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, timezone.utc) - timedelta(hours=3)
        return dt.strftime(datefmt if datefmt else "%Y-%m-%d %H:%M:%S")


handler = logging.StreamHandler()
handler.setFormatter(BrazilFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(handler)

# --- CONFIGURAÇÕES DE AMBIENTE ---
DB_HOST = os.getenv("DB_HOST", "patroni-primary")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "relatorio_meta_ads")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID_LIST = os.getenv("AD_ACCOUNTS", "").split(",")

API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def get_date_range():
    # Forçando o robô a buscar desde 01 de Dezembro até hoje
    # Depois que ele rodar uma vez, você volta para a versão anterior
    return "2025-12-01", datetime.today().strftime("%Y-%m-%d")


# def get_date_range():     # Versão anterior, voltar pra ele apartir de amanhã
#     today = datetime.today()
#     first = today.replace(day=1)
#     previous_month = (first - timedelta(days=1)).replace(day=1)
#     return previous_month.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def clear_existing_data(account_id, since, until):
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
    if not raw_data_page:
        return
    df = pd.DataFrame(raw_data_page)
    df["account_id"] = account_id
    df["nome_conta"] = f"Conta {account_id}"

    # Mapeamento robusto baseado no diagnóstico de API realizado pelo Luan
    mapping = {
        "lead": [
            "lead",
            "onsite_conversion.lead_grouped",
            "offsite_conversion.fb_pixel_lead",
            "onsite_web_lead",
            "onsite_conversion.lead",
            "offsite_complete_registration_add_meta_leads",
        ],
        "lp_view": ["landing_page_view", "omni_landing_page_view"],
        "conversas_iniciadas": ["onsite_conversion.messaging_conversation_started_7d"],
        "novos_contatos_mensagem": ["onsite_conversion.messaging_first_reply"],
        "compras": [
            "purchase",
            "onsite_web_purchase",
            "offsite_conversion.fb_pixel_purchase",
            "omni_purchase",
        ],
        "videoview_3s": ["video_view"],
        "cliques_saida": ["outbound_click", "link_click"],
    }

    # Processamento de métricas numéricas simples
    for col in ["impressions", "spend"]:
        df[col] = pd.to_numeric(df.get(col, 0)).fillna(0)

    # Processamento de ações (Conversões) - Somando múltiplos tipos
    if "actions" in df.columns:
        for target_col, api_keys in mapping.items():
            df[target_col] = df["actions"].apply(
                lambda x: sum(
                    [
                        float(item.get("value", 0))
                        for item in x
                        if item.get("action_type") in api_keys
                    ]
                )
                if isinstance(x, list)
                else 0.0
            )
    else:
        for target_col in mapping.keys():
            df[target_col] = 0.0

    # Inicialização de colunas extras para manter compatibilidade com o banco
    df["valor_compra"] = 0.0
    df["videoview_50"] = 0.0
    df["videoview_75"] = 0.0
    df["contato"] = 0.0
    df["initiate_checkout"] = 0.0
    df["seguidores_instagram"] = 0.0
    df["visitas_perfil"] = 0.0

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
            "cliques_saida": "clique_link",  # Mapeado para clique_link no banco
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
    clear_existing_data(clean_id, since, until)

    url = f"{BASE_URL}/{clean_id}/insights"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "fields": "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,spend,actions",
        "breakdowns": "publisher_platform,platform_position",
        "limit": 25,  # Mantido em 25 para evitar os timeouts que vimos na v2/v3
    }

    page, total = 0, 0
    while True:
        try:
            page += 1
            response = requests.get(url, params=params, timeout=60)
            if response.status_code != 200:
                logger.error(f"❌ Erro API ({clean_id}): {response.text}")
                break
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                transform_and_load(data["data"], clean_id)
                count = len(data["data"])
                total += count
                logger.info(
                    f"   💾 Pág {page} salva (+{count} regs) | Total conta: {total}"
                )

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
    logger.info("🚀 INICIANDO ETL (v6 - Mapeamento de Leads Otimizado)")
    since, until = get_date_range()
    accounts = [acc.strip() for acc in AD_ACCOUNT_ID_LIST if acc.strip()]
    for account_id in accounts:
        fetch_and_process(account_id, since, until)
    logger.info("✅ JOB FINALIZADO - Próxima execução em 4h")


run_etl()
schedule.every(4).hours.do(run_etl)
while True:
    schedule.run_pending()
    time.sleep(60)
