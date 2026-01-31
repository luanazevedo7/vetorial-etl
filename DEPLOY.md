# Guia de Deploy - ETL Meta Ads

## 📋 Checklist Pré-Deploy

Antes de fazer o deploy, certifique-se de ter:

- [ ] PostgreSQL acessível via HAProxy no Swarm
- [ ] Meta Ads Access Token válido
- [ ] IDs das contas de anúncios (formato: `act_123456789`)
- [ ] Docker Swarm inicializado
- [ ] Rede `internal_network` criada no Swarm
- [ ] Acesso ao Docker Hub (ou registry privado)

## 🗄️ Passo 1: Preparar Banco de Dados

### 1.1 Criar Database (se não existir)

```bash
# Conecte-se ao PostgreSQL master
docker exec -it $(docker ps -q -f name=postgres_master) psql -U postgres

# Dentro do psql:
CREATE DATABASE relatorio_meta_ads;
\c relatorio_meta_ads
\q
```

### 1.2 Executar Schema

**Opção A: Via psql (linha de comando)**

```bash
psql -h haproxy -U postgres -d relatorio_meta_ads -f schema.sql
```

**Opção B: Via DBeaver/pgAdmin**

1. Conecte-se ao servidor (HAProxy:5432)
2. Abra/copie o conteúdo de `schema.sql`
3. Execute o script

**Validar tabela criada:**

```sql
\dt insights_meta_ads
SELECT COUNT(*) FROM insights_meta_ads;
```

## 🔑 Passo 2: Configurar Secrets (Recomendado para Produção)

### 2.1 Criar Docker Secrets

```bash
# Senha do banco
echo "sua_senha_postgres" | docker secret create db_password -

# Token Meta Ads
echo "seu_token_meta_ads" | docker secret create meta_token -

# Lista de contas
echo "act_123456789,act_987654321" | docker secret create ad_accounts -
```

### 2.2 Atualizar docker-compose.yml (se usar secrets)

```yaml
services:
  meta_etl_worker:
    # ... outras configurações
    secrets:
      - db_password
      - meta_token
      - ad_accounts
    environment:
      - DB_PASS_FILE=/run/secrets/db_password
      - META_ACCESS_TOKEN_FILE=/run/secrets/meta_token
      - AD_ACCOUNTS_FILE=/run/secrets/ad_accounts

secrets:
  db_password:
    external: true
  meta_token:
    external: true
  ad_accounts:
    external: true
```

**Nota:** Se usar secrets, ajuste `main.py` para ler de arquivos:

```python
# Exemplo de leitura de secret
def read_secret(secret_name):
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            return f.read().strip()
    return os.getenv(secret_name.upper())

DB_PASS = read_secret("db_password")
```

## 🔧 Passo 3: Configurar Variáveis de Ambiente (Alternativa Simples)

Se não usar secrets:

```bash
# Crie o arquivo .env
cp .env.example .env

# Edite com suas credenciais
nano .env
```

Conteúdo do `.env`:

```env
DB_HOST=haproxy
DB_PORT=5432
DB_NAME=relatorio_meta_ads
DB_USER=postgres
DB_PASS=SuaSenhaPostgres123

META_ACCESS_TOKEN=EAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AD_ACCOUNTS=act_123456789,act_987654321

DOCKER_USERNAME=seu-usuario-dockerhub
```

## 🔍 Passo 4: Validar Action Types (Altamente Recomendado)

Antes do deploy, descubra os action_types reais das suas contas:

```bash
# Carregue as variáveis de ambiente
export $(cat .env | xargs)

# Execute o script de descoberta
python discovery.py
```

**Saída esperada:**

```
===========================================
CONTA: act_123456789
===========================================

--- ACTIONS DISPONÍVEIS ---
  landing_page_view                      | Valor: 1234
  lead                                   | Valor: 567
  link_click                             | Valor: 8901
  ...
```

**Se alguma métrica importante vier zerada no ETL, compare com esta saída!**

## 🐳 Passo 5: Build e Push da Imagem

### 5.1 Build Local

```bash
# Build
docker build -t seu-usuario/etl-meta-ads:latest .

# Login no Docker Hub
docker login

# Push
docker push seu-usuario/etl-meta-ads:latest
```

### 5.2 Build via GitHub Actions (Recomendado)

1. Configure secrets no repositório GitHub:
   - Settings → Secrets and variables → Actions
   - Adicione:
     - `DOCKERHUB_USERNAME`
     - `DOCKERHUB_TOKEN` (gere em hub.docker.com → Account Settings → Security)

2. Faça push do código:

```bash
git add .
git commit -m "feat: initial ETL setup"
git push origin main
```

3. A Action fará o build automaticamente. Acompanhe em "Actions" no GitHub.

## 🚀 Passo 6: Deploy no Swarm

### 6.1 Criar Network (se não existir)

```bash
docker network create --driver overlay internal_network
```

### 6.2 Deploy da Stack

```bash
# Com variáveis de ambiente do .env
export $(cat .env | xargs)
docker stack deploy -c docker-compose.yml etl-meta
```

### 6.3 Verificar Deploy

```bash
# Listar serviços
docker service ls | grep etl-meta

# Ver logs em tempo real
docker service logs -f etl-meta_meta_etl_worker

# Ver replicas
docker service ps etl-meta_meta_etl_worker
```

**Saída esperada nos logs:**

```
============================================================
INICIANDO JOB ETL META ADS
============================================================
Período de extração: 2026-01-01 até 2026-01-31
Contas a processar: 2

--- Processando conta: act_123456789 ---
Conta act_123456789: Página 1 - 100 registros
...
Transformados 1234 registros para conta act_123456789
Deletados 1150 registros antigos para conta act_123456789
Inseridos 1234 registros para conta act_123456789
============================================================
JOB FINALIZADO - Aguardando próximo ciclo
============================================================
```

## ✅ Passo 7: Validar Dados no Banco

```sql
-- Conecte ao PostgreSQL
psql -h haproxy -U postgres -d relatorio_meta_ads

-- Validar inserção
SELECT
    account_id,
    COUNT(*) as total_registros,
    MIN(data_registro) as data_min,
    MAX(data_registro) as data_max,
    SUM(impressoes) as total_impressoes,
    SUM(valor_gasto) as total_gasto
FROM insights_meta_ads
GROUP BY account_id;
```

**Resultado esperado:**

```
  account_id   | total_registros |  data_min  |  data_max  | total_impressoes | total_gasto
---------------+-----------------+------------+------------+------------------+-------------
 act_123456789 |            1234 | 2026-01-01 | 2026-01-31 |          1234567 |     5678.90
```

## 🔄 Passo 8: Atualizar Serviço (após mudanças de código)

```bash
# Após push de nova versão no GitHub
docker service update --image seu-usuario/etl-meta-ads:latest etl-meta_meta_etl_worker

# Ou forçar re-pull
docker service update --force etl-meta_meta_etl_worker
```

## 🐛 Troubleshooting

### Erro: "relation insights_meta_ads does not exist"

**Solução:** Você esqueceu de rodar o `schema.sql`. Execute o Passo 1.2.

### Erro: "FATAL: password authentication failed"

**Solução:** Verifique `DB_USER` e `DB_PASS` no `.env`.

### Erro: Rate limit 100%

**Solução:** Aguarde 2 minutos. O script pausa automaticamente.

### Dados zerados em algumas colunas

**Solução:**

1. Execute `python discovery.py`
2. Compare os `action_type` retornados com os mapeados em `main.py`
3. Ajuste o mapeamento se necessário

### Container reiniciando constantemente

**Solução:**

```bash
# Veja o erro exato
docker service logs etl-meta_meta_etl_worker --tail 50

# Geralmente é credencial inválida ou tabela inexistente
```

## 📊 Monitoramento Contínuo

```bash
# Ver uso de recursos
docker stats $(docker ps -q -f name=etl-meta)

# Ver próxima execução agendada (nos logs)
docker service logs etl-meta_meta_etl_worker | grep "agendado"
```

## 🎯 Checklist Pós-Deploy

- [ ] Tabela criada e acessível
- [ ] Primeira execução concluída sem erros
- [ ] Dados visíveis no banco (validação SQL)
- [ ] Logs não mostram erros críticos
- [ ] Rate limit abaixo de 80%
- [ ] Agendamento funcionando (próxima execução em 4 horas)

---

✅ **Deploy concluído com sucesso!** O ETL rodará automaticamente a cada 4 horas.
