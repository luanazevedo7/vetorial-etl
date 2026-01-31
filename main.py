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
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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

# API Version v24.0
API_VERSION = "v24.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# Conexão com Banco de Dados
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_pre_ping=True)


def get_date_range():
    """Retorna data de início (1º dia do mês anterior) e fim (hoje)"""
    today = datetime.today()
    first = today.replace(day=1)
    previous_month = (first - timedelta(days=1)).replace(day=1)
    
    since = previous_month.strftime('%Y-%m-%d')
    until = today.strftime('%Y-%m-%d')
    
    logger.info(f"Período de extração: {since} até {until}")
    return since, until


def check_rate_limit(headers):
    """Verifica headers para evitar bloqueio da API"""
    if 'x-fb-ads-insights-throttle' in headers:
        try:
            throttle_data = json.loads(headers['x-fb-ads-insights-throttle'])
            acc_util = throttle_data.get('acc_id_util_pct', 0)
            app_util = throttle_data.get('app_id_util_pct', 0)
            
            logger.info(f"Rate limit - App: {app_util}% | Account: {acc_util}%")
            
            if acc_util > 80 or app_util > 80:
                logger.warning(f"Rate limit alto. Pausando por 120 segundos...")
                time.sleep(120)
        except Exception as e:
            logger.error(f"Erro ao parsear rate limit: {e}")


def fetch_meta_data(account_id, since, until):
    """Busca dados na API da Meta com paginação"""
    url = f"{BASE_URL}/{account_id}/insights"
    
    fields = [
        "campaign_id", "campaign_name",
        "adset_id", "adset_name",
        "ad_id", "ad_name",
        "impressions", "spend", "inline_link_clicks",
        "actions", "action_values", 
        "video_p50_watched_actions", 
        "video_p75_watched_actions",
        "video_play_actions"
    ]
    
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "fields": ",".join(fields),
        "breakdowns": "publisher_platform,platform_position",
        "limit": 100
    }

    all_data = []
    page_count = 0
    
    while True:
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            check_rate_limit(response.headers)
            
            if 'data' in data:
                all_data.extend(data['data'])
                page_count += 1
                logger.info(f"Conta {account_id}: Página {page_count} - {len(data['data'])} registros")
            
            # Paginação
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                params = {}
            else:
                break
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição para conta {account_id}: {e}")
            break
    
    logger.info(f"Total extraído da conta {account_id}: {len(all_data)} registros")
    return all_data


def process_actions(row, action_type_target):
    """Função auxiliar para extrair valor de listas de dicionários (actions)"""
    if isinstance(row, list):
        for item in row:
            if item.get('action_type') == action_type_target:
                try:
                    return float(item.get('value', 0))
                except (ValueError, TypeError):
                    return 0.0
    return 0.0


def transform_data(raw_data, account_id):
    """Transforma dados da API em DataFrame normalizado"""
    if not raw_data:
        logger.warning(f"Nenhum dado bruto para transformar da conta {account_id}")
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    
    # Identificadores
    df['account_id'] = account_id
    df['nome_conta'] = f"Conta {account_id.replace('act_', '')}"

    # Métricas Diretas
    df['impressoes'] = pd.to_numeric(df.get('impressions', 0), errors='coerce').fillna(0).astype(int)
    df['valor_gasto'] = pd.to_numeric(df.get('spend', 0), errors='coerce').fillna(0)
    df['clique_link'] = pd.to_numeric(df.get('inline_link_clicks', 0), errors='coerce').fillna(0).astype(int)
    
    # Métricas de Ação
    df['lp_view'] = df.get('actions', []).apply(lambda x: process_actions(x, 'landing_page_view'))
    df['lead'] = df.get('actions', []).apply(lambda x: process_actions(x, 'lead'))
    df['contato'] = df.get('actions', []).apply(lambda x: process_actions(x, 'contact'))
    df['conversas_iniciadas'] = df.get('actions', []).apply(
        lambda x: process_actions(x, 'onsite_conversion.messaging_conversation_started_7d')
    )
    df['novos_contatos_mensagem'] = df.get('actions', []).apply(
        lambda x: process_actions(x, 'onsite_conversion.messaging_first_reply')
    )
    df['seguidores_instagram'] = df.get('actions', []).apply(lambda x: process_actions(x, 'follow'))
    df['visitas_perfil'] = df.get('actions', []).apply(lambda x: process_actions(x, 'onsite_conversion.post_save'))
    df['initiate_checkout'] = df.get('actions', []).apply(lambda x: process_actions(x, 'initiate_checkout'))
    df['compras'] = df.get('actions', []).apply(lambda x: process_actions(x, 'purchase'))
    df['cliques_saida'] = df.get('actions', []).apply(lambda x: process_actions(x, 'outbound_click'))
    
    # Valor de Compra
    df['valor_compra'] = df.get('action_values', []).apply(lambda x: process_actions(x, 'purchase'))
    
    # Vídeo Views
    df['videoview_50'] = df.get('video_p50_watched_actions', []).apply(lambda x: process_actions(x, 'video_view'))
    df['videoview_75'] = df.get('video_p75_watched_actions', []).apply(lambda x: process_actions(x, 'video_view'))
    df['videoview_3s'] = df.get('video_play_actions', []).apply(lambda x: process_actions(x, 'video_view'))

    # Renomear colunas
    df.rename(columns={
        'campaign_id': 'id_campanha',
        'campaign_name': 'campanha',
        'adset_id': 'id_conjunto_anuncios',
        'adset_name': 'conjunto_anuncios',
        'ad_id': 'id_anuncio',
        'ad_name': 'anuncio',
        'date_start': 'data_registro',
        'publisher_platform': 'plataforma',
        'platform_position': 'posicionamento'
    }, inplace=True)

    # Garantir colunas numéricas
    numeric_cols = [
        'impressoes', 'cliques_saida', 'clique_link', 'lp_view', 'lead', 
        'contato', 'conversas_iniciadas', 'novos_contatos_mensagem', 
        'seguidores_instagram', 'visitas_perfil', 'initiate_checkout', 
        'compras', 'valor_compra', 'videoview_3s', 'videoview_50', 
        'videoview_75', 'valor_gasto'
    ]
    
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Seleção Final
    final_cols = [
        'account_id', 'nome_conta', 'id_campanha', 'id_conjunto_anuncios', 'id_anuncio',
        'campanha', 'conjunto_anuncios', 'anuncio', 'impressoes', 'cliques_saida',
        'clique_link', 'lp_view', 'lead', 'contato', 'conversas_iniciadas',
        'novos_contatos_mensagem', 'seguidores_instagram', 'visitas_perfil',
        'initiate_checkout', 'compras', 'valor_compra', 'data_registro',
        'videoview_3s', 'videoview_50', 'videoview_75', 'plataforma',
        'posicionamento', 'valor_gasto'
    ]
    
    for col in final_cols:
        if col not in df.columns:
            df[col] = None if col in ['plataforma', 'posicionamento', 'data_registro'] else 0
    
    result_df = df[final_cols]
    logger.info(f"Transformados {len(result_df)} registros para conta {account_id}")
    return result_df


def load_to_postgres(df, account_id, since, until):
    """Delete (janela de tempo) + Insert"""
    if df.empty:
        logger.warning("DataFrame vazio. Nada a inserir.")
        return

    try:
        with engine.begin() as conn:
            # 1. Deletar dados do período
            delete_query = text("""
                DELETE FROM insights_meta_ads 
                WHERE account_id = :acc_id 
                AND data_registro >= :since 
                AND data_registro <= :until
            """)
            result = conn.execute(delete_query, {
                'acc_id': account_id, 
                'since': since, 
                'until': until
            })
            logger.info(f"Deletados {result.rowcount} registros antigos para conta {account_id}")

            # 2. Inserir novos dados
            df.to_sql(
                'insights_meta_ads', 
                conn, 
                if_exists='append', 
                index=False, 
                method='multi', 
                chunksize=500
            )
            logger.info(f"Inseridos {len(df)} registros para conta {account_id}")
            
    except Exception as e:
        logger.error(f"Erro ao carregar dados no PostgreSQL: {e}")
        raise


def run_etl():
    """Executa o pipeline ETL completo"""
    logger.info("=" * 60)
    logger.info("INICIANDO JOB ETL META ADS")
    logger.info("=" * 60)
    
    since, until = get_date_range()
    accounts = [acc.strip() for acc in AD_ACCOUNT_ID_LIST if acc.strip()]
    
    if not accounts:
        logger.error("Nenhuma conta configurada em AD_ACCOUNTS")
        return
    
    logger.info(f"Contas a processar: {len(accounts)}")
    
    for account_id in accounts:
        logger.info(f"\n--- Processando conta: {account_id} ---")
        
        try:
            # Extract
            raw_data = fetch_meta_data(account_id, since, until)
            
            # Transform
            if raw_data:
                df_processed = transform_data(raw_data, account_id)
                
                # Load
                if not df_processed.empty:
                    load_to_postgres(df_processed, account_id, since, until)
                else:
                    logger.warning(f"DataFrame processado vazio para {account_id}")
            else:
                logger.warning(f"Sem dados retornados da API para {account_id}")
                
        except Exception as e:
            logger.error(f"Erro ao processar conta {account_id}: {e}", exc_info=True)
            continue
    
    logger.info("=" * 60)
    logger.info("JOB FINALIZADO - Aguardando próximo ciclo")
    logger.info("=" * 60)


if __name__ == "__main__":
    # Executar imediatamente ao iniciar
    run_etl()
    
    # Agendar a cada 4 horas
    schedule.every(4).hours.do(run_etl)
    
    logger.info("ETL agendado para rodar a cada 4 horas")
    
    while True:
        schedule.run_pending()
        time.sleep(60)
