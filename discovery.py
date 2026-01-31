"""
Script de Descoberta de Action Types
Execute este script ANTES de rodar o ETL em produção para validar
quais action_types realmente existem nas suas contas Meta Ads
"""

import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNTS = os.getenv("AD_ACCOUNTS", "").split(",")
API_VERSION = "v24.0"


def discover_action_types(account_id):
    """Descobre todos os action_types disponíveis em uma conta"""
    url = f"https://graph.facebook.com/{API_VERSION}/{account_id}/insights"

    params = {
        "access_token": ACCESS_TOKEN,
        "level": "account",
        "date_preset": "last_30d",
        "fields": "actions,action_values",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "data" in data and len(data["data"]) > 0:
            print(f"\n{'=' * 80}")
            print(f"CONTA: {account_id}")
            print(f"{'=' * 80}")

            # Actions
            actions = data["data"][0].get("actions", [])
            if actions:
                print("\n--- ACTIONS DISPONÍVEIS ---")
                for action in sorted(actions, key=lambda x: x["action_type"]):
                    print(f"  {action['action_type']:<60} | Valor: {action['value']}")
            else:
                print("Nenhuma action encontrada")

            # Action Values
            action_values = data["data"][0].get("action_values", [])
            if action_values:
                print("\n--- ACTION VALUES DISPONÍVEIS ---")
                for av in sorted(action_values, key=lambda x: x["action_type"]):
                    print(f"  {av['action_type']:<60} | Valor: {av['value']}")
            else:
                print("Nenhum action_value encontrado")

        else:
            print(f"\nConta {account_id}: Sem dados ou erro na resposta")
            print(f"Resposta completa: {json.dumps(data, indent=2)}")

    except requests.exceptions.RequestException as e:
        print(f"\nErro ao consultar conta {account_id}: {e}")
    except Exception as e:
        print(f"\nErro inesperado para conta {account_id}: {e}")


def main():
    """Executa descoberta para todas as contas configuradas"""
    print("\n" + "=" * 80)
    print("SCRIPT DE DESCOBERTA DE ACTION TYPES - META ADS API")
    print("=" * 80)

    if not ACCESS_TOKEN:
        print("\n❌ ERRO: META_ACCESS_TOKEN não configurado no .env")
        return

    accounts = [acc.strip() for acc in AD_ACCOUNTS if acc.strip()]

    if not accounts:
        print("\n❌ ERRO: AD_ACCOUNTS não configurado no .env")
        return

    print(f"\nContas a analisar: {len(accounts)}")

    for account_id in accounts:
        discover_action_types(account_id)

    print("\n" + "=" * 80)
    print("ANÁLISE CONCLUÍDA")
    print("=" * 80)
    print("\nUSE OS RESULTADOS ACIMA PARA AJUSTAR O MAPEAMENTO EM main.py")
    print("Especialmente para métricas customizadas que possam ter nomes diferentes")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
