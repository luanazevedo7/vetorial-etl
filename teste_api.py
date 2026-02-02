import requests
import json

# --- COLOQUE SEUS DADOS AQUI ---
TOKEN = "SEU_TOKEN_AQUI"  # Certifique-se de que o token está entre aspas
CONTA = "act_213844638058297"

url = f"https://graph.facebook.com/v21.0/{CONTA}/insights"
params = {
    "access_token": "TOKEN",
    "level": "account",
    "date_preset": "last_30d",
    "fields": "actions,impressions,spend",
}

print(f"DEBUG: Tentando conectar na Meta...")
try:
    resp = requests.get(url, params=params, timeout=15)
    print(f"DEBUG: Status Code: {resp.status_code}")

    data = resp.json()

    if "error" in data:
        print(f"❌ ERRO DA META: {data['error']['message']}")
    elif "data" in data and len(data["data"]) > 0:
        print("\n✅ CONEXÃO OK! DADOS ENCONTRADOS:")
        print(f"Gasto total nos últimos 30 dias: {data['data'][0].get('spend')}")

        actions = data["data"][0].get("actions", [])
        print("-" * 50)
        for act in actions:
            print(f"Evento: {act['action_type']} | Valor: {act['value']}")
        print("-" * 50)
    else:
        print(
            "❓ A conta conectou, mas não retornou 'actions'. Pode ser que não houve conversões no período."
        )
        print("Resposta completa:", json.dumps(data, indent=2))

except Exception as e:
    print(f"❌ ERRO NO SCRIPT: {e}")
