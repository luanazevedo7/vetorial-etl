import os
import time
import requests
import pandas as pd
import schedule
import logging
import pytz  # Para corrigir a hora
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import json


# --- CONFIGURAÇÃO DE LOGS (HORA BRASIL) ---
def brazil_time(*args):
    return datetime.now(pytz.timezone("America/Sao_Paulo")).timetuple()


logging.Formatter.converter = brazil_time
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

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
    """Apaga os dados do período ANTES de começar a baixar, para evitar duplicidade"""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM insights_meta_ads WHERE account_id = :acc AND data_registro >= :s AND data_registro <= :u"
                ),
                {"acc": account_id, "s": since, "u": until},
            )
        logger.info(f"🧹 Limpeza inicial realizada para a conta {account_id}")
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")


def transform_and_load(raw_data_page, account_id):
    """Processa UMA página e salva imediatamente para liberar memória"""
    if not raw_data_page:
        return

    df = pd.DataFrame(raw_data_page)
    df["account_id"] = account_id
    df["nome_conta"] = f"Conta {account_id}"

    # Funções auxiliares de transformação
    def process_actions(row, action_type_target):
        if isinstance(row, list):
            for item in row:
                if item.get("action_type") == action_type_target:
                    return float(item.get("value", 0))
        return 0.0

    # Tratamento numérico
    df["impressoes"] = pd.to_numeric(df.get("impressions", 0)).fillna(0)
    df["valor_gasto"] = pd.to_numeric(df.get("spend", 0)).fillna(0)
    df["clique_link"] = pd.to_numeric(df.get("inline_link_clicks", 0)).fillna(0)

    # Extração de Actions
    if "actions" in df.columns:
        df["lp_view"] = df["actions"].apply(
            lambda x: process_actions(x, "landing_page_view")
        )
        df["lead"] = df["actions"].apply(lambda x: process_actions(x, "lead"))
        df["contato"] = df["actions"].apply(lambda x: process_actions(x, "contact"))
        df["conversas_iniciadas"] = df["actions"].apply(
            lambda x: process_actions(
                x, "onsite_conversion.messaging_conversation_started_7d"
            )
        )
        df["novos_contatos_mensagem"] = df["actions"].apply(
            lambda x: process_actions(x, "onsite_conversion.messaging_first_reply")
        )
        df["seguidores_instagram"] = df["actions"].apply(
            lambda x: process_actions(x, "follow")
        )
        df["visitas_perfil"] = df["actions"].apply(
            lambda x: process_actions(x, "profile_visit")
        )
        df["initiate_checkout"] = df["actions"].apply(
            lambda x: process_actions(x, "initiate_checkout")
        )
        df["compras"] = df["actions"].apply(lambda x: process_actions(x, "purchase"))
        df["cliques_saida"] = df["actions"].apply(
            lambda x: process_actions(x, "outbound_click")
        )
        df["videoview_3s"] = df["actions"].apply(
            lambda x: process_actions(x, "video_view")
        )
    else:
        # Se actions não vier, preenche tudo com 0
        cols_actions = [
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
        for c in cols_actions:
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

    # SALVAMENTO IMEDIATO DA PÁGINA
    try:
        with engine.begin() as conn:
            df[final_cols].to_sql(
                "insights_meta_ads",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
    except Exception as e:
        logger.error(f"Erro ao salvar página no banco: {e}")


def fetch_and_process(account_id, since, until):
    clean_id = account_id.strip()
    if not clean_id.startswith("act_"):
        clean_id = f"act_{clean_id}"

    # 1. LIMPA DADOS ANTIGOS ANTES DE COMEÇAR
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
        "limit": 50,  # Mantém lote pequeno para economizar memória
    }

    page_count = 0
    total_saved = 0

    while True:
        try:
            page_count += 1
            response = requests.get(url, params=params, timeout=60)

            if response.status_code != 200:
                logger.error(f"❌ ERRO META API (Conta {clean_id}): {response.text}")
                break  # Para essa conta e vai para a próxima

            data = response.json()

            if "data" in data and len(data["data"]) > 0:
                # PROCESSA E SALVA AGORA!
                transform_and_load(data["data"], clean_id)
                current_batch = len(data["data"])
                total_saved += current_batch
                logger.info(
                    f"   💾 Página {page_count} salva: +{current_batch} registros (Total na conta: {total_saved})"
                )

            # Rate Limit
            if "x-fb-ads-insights-throttle" in response.headers:
                try:
                    throttle = json.loads(
                        response.headers["x-fb-ads-insights-throttle"]
                    )
                    if throttle.get("acc_id_util_pct", 0) > 90:
                        logger.warning("⚠️ Rate limit alto. Pausando 3 min...")
                        time.sleep(180)
                except:
                    pass

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
                params = {}
            else:
                logger.info(
                    f"🏁 Fim da conta {clean_id}. Total processado: {total_saved} registros."
                )
                break

        except Exception as e:
            logger.error(f"❌ Falha fatal na página {page_count}: {e}")
            break


def run_etl():
    logger.info("INICIANDO JOB ETL META ADS (Modo Streaming - Baixa RAM)")
    since, until = get_date_range()
    accounts = [acc.strip() for acc in AD_ACCOUNT_ID_LIST if acc.strip()]

    for account_id in accounts:
        logger.info(f"--- Processando conta: {account_id} ---")
        fetch_and_process(account_id, since, until)

    logger.info("JOB FINALIZADO - Aguardando 4 horas")


# Execução
run_etl()
schedule.every(4).hours.do(run_etl)
while True:
    schedule.run_pending()
    time.sleep(60)
