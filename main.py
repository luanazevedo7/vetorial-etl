import os
import time
import requests
import pandas as pd
import schedule
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import json

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE AMBIENTE ---
DB_HOST = os.getenv("DB_HOST", "haproxy")
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


def check_rate_limit(headers):
    if "x-fb-ads-insights-throttle" in headers:
        try:
            throttle = json.loads(headers["x-fb-ads-insights-throttle"])
            acc_util = throttle.get("acc_id_util_pct", 0)
            if acc_util > 80:
                logger.warning(f"Rate limit alto: {acc_util}%. Pausa de 2 min.")
                time.sleep(120)
        except:
            pass


def fetch_meta_data(account_id, since, until):
    # 1. GARANTIA DO PREFIXO act_
    clean_id = account_id.strip()
    if not clean_id.startswith("act_"):
        clean_id = f"act_{clean_id}"

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
        "action_values",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
    ]

    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "fields": ",".join(fields),
        "breakdowns": "publisher_platform,platform_position",
        "limit": 50,  # Reduzi para 50 para evitar Timeout da Meta em queries pesadas
    }

    all_data = []
    page_count = 0  # Contador de páginas

    while True:
        try:
            page_count += 1
            if page_count % 5 == 0:  # Avisa a cada 5 páginas para não poluir demais
                logger.info(
                    f"   ⏳ Baixando página {page_count} da conta {clean_id}..."
                )

            # Adicionei timeout=60s para não ficar travado infinitamente se a rede cair
            response = requests.get(url, params=params, timeout=60)

            if response.status_code != 200:
                logger.error(f"❌ ERRO META API (Conta {clean_id}): {response.text}")
                response.raise_for_status()

            data = response.json()

            if "data" in data:
                current_batch = len(data["data"])
                all_data.extend(data["data"])
                # Logger detalhado para ver se está andando
                logger.info(
                    f"   ✅ Página {page_count}: +{current_batch} registros (Total: {len(all_data)})"
                )

            # Rate Limit Check
            if "x-fb-ads-insights-throttle" in response.headers:
                try:
                    throttle = json.loads(
                        response.headers["x-fb-ads-insights-throttle"]
                    )
                    acc_util = throttle.get("acc_id_util_pct", 0)
                    if acc_util > 90:
                        logger.warning(
                            f"⚠️ Rate limit alto ({acc_util}%). Pausando 3 min..."
                        )
                        time.sleep(180)
                except:
                    pass

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
                params = {}
            else:
                logger.info(
                    f"🏁 Fim da paginação. Total extraído: {len(all_data)} registros."
                )
                break

        except requests.exceptions.Timeout:
            logger.error(
                f"❌ Timeout ao baixar página {page_count}. Tentando novamente em 30s..."
            )
            time.sleep(30)
            continue  # Tenta a mesma página de novo

        except Exception as e:
            logger.error(f"❌ Falha fatal na requisição: {e}")
            break

    return all_data


def process_actions(row, action_type_target):
    if isinstance(row, list):
        for item in row:
            if item.get("action_type") == action_type_target:
                return float(item.get("value", 0))
    return 0.0


def transform_data(raw_data, account_id):
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    df["account_id"] = account_id
    df["nome_conta"] = f"Conta {account_id}"

    # Tratamento numérico
    df["impressoes"] = pd.to_numeric(df.get("impressions", 0)).fillna(0)
    df["valor_gasto"] = pd.to_numeric(df.get("spend", 0)).fillna(0)
    df["clique_link"] = pd.to_numeric(df.get("inline_link_clicks", 0)).fillna(0)

    # Extração de Actions
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

    # Video Views (3s vem dentro de actions como video_view)
    df["videoview_3s"] = df["actions"].apply(lambda x: process_actions(x, "video_view"))

    # Campos que removemos da API, preenchemos com 0 para não quebrar o banco
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

    # Preencher nulos finais
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

    return df[final_cols]


def load_to_postgres(df, account_id, since, until):
    if df.empty:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM insights_meta_ads WHERE account_id = :acc AND data_registro >= :s AND data_registro <= :u"
            ),
            {"acc": account_id, "s": since, "u": until},
        )
        df.to_sql(
            "insights_meta_ads",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        logger.info(f"✅ {len(df)} registros salvos no banco!")


def run_etl():
    logger.info("INICIANDO JOB ETL META ADS")
    since, until = get_date_range()
    accounts = [acc.strip() for acc in AD_ACCOUNT_ID_LIST if acc.strip()]

    for account_id in accounts:
        logger.info(f"--- Processando conta: {account_id} ---")
        data = fetch_meta_data(account_id, since, until)
        if data:
            df = transform_data(data, account_id)
            load_to_postgres(df, account_id, since, until)
        else:
            logger.warning(f"Nenhum dado encontrado para {account_id}")

    logger.info("JOB FINALIZADO - Aguardando 4 horas")


# Execução
run_etl()
schedule.every(4).hours.do(run_etl)
while True:
    schedule.run_pending()
    time.sleep(60)
