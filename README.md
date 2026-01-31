# ETL Meta Ads - Pipeline de Dados

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Docker](https://img.shields.io/badge/docker-ready-brightgreen.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

Pipeline ETL automatizado para extrair, transformar e carregar dados da **Meta Ads API** (Facebook/Instagram) em PostgreSQL via Docker Swarm.

## 📋 Características

- ✅ **Extração automática** da Meta Ads API v24.0
- ✅ **Normalização de métricas** de actions e video views
- ✅ **Delete + Insert** com janela de tempo para garantir dados atualizados
- ✅ **Rate limiting** automático para não exceder limites da API
- ✅ **Docker Swarm ready** com resource limits
- ✅ **CI/CD** via GitHub Actions
- ✅ **Logs estruturados** para debugging
- ✅ **Agendamento** a cada 4 horas

## 🏗️ Arquitetura

```
Meta Ads API (v24.0)
        ↓
   [Extract] → Paginação + Rate Limit Check
        ↓
  [Transform] → Normalização de Actions/Métricas
        ↓
    [Load] → PostgreSQL (Delete + Insert)
        ↓
  Docker Swarm (HAProxy + Postgres Cluster)
```

## 📦 Estrutura do Projeto

```
etl-vetorial/
├── main.py                 # Script ETL principal
├── discovery.py            # Script de descoberta de action_types
├── schema.sql              # Schema da tabela PostgreSQL
├── requirements.txt        # Dependências Python
├── Dockerfile              # Imagem Docker otimizada
├── docker-compose.yml      # Stack do Swarm
├── .env.example            # Template de variáveis de ambiente
├── .gitignore              # Arquivos ignorados pelo Git
├── .github/
│   └── workflows/
│       └── docker-build.yml  # CI/CD automático
└── README.md               # Esta documentação
```

## 🚀 Instalação e Deploy

### 1️⃣ Pré-requisitos

- Docker Swarm configurado
- PostgreSQL acessível via HAProxy
- Meta Ads Access Token com permissão `ads_read`
- Conta Docker Hub (para CI/CD)

### 2️⃣ Configuração do Banco de Dados

Execute o schema SQL no PostgreSQL antes do primeiro deploy:

```bash
psql -h haproxy -U postgres -d relatorio_meta_ads -f schema.sql
```

**Ou via DBeaver/pgAdmin:**

1. Conecte-se ao PostgreSQL via HAProxy
2. Abra o arquivo `schema.sql`
3. Execute o script

### 3️⃣ Configuração de Variáveis de Ambiente

1. Copie o template:

```bash
cp .env.example .env
```

2. Edite o `.env` com suas credenciais:

```env
DB_HOST=haproxy
DB_PORT=5432
DB_NAME=relatorio_meta_ads
DB_USER=seu_usuario
DB_PASS=sua_senha_segura

META_ACCESS_TOKEN=seu_token_aqui
AD_ACCOUNTS=act_123456789,act_987654321
```

> ⚠️ **IMPORTANTE**: Nunca commite o arquivo `.env` no Git!

### 4️⃣ Descoberta de Action Types (Opcional mas Recomendado)

Antes de rodar em produção, valide quais `action_types` existem nas suas contas:

```bash
python discovery.py
```

Isso mostrará todos os eventos disponíveis. Ajuste o mapeamento em `main.py` se necessário.

### 5️⃣ Deploy no Docker Swarm

#### Opção A: Build Local

```bash
# Build da imagem
docker build -t seu-usuario/etl-meta-ads:latest .

# Push para Docker Hub
docker push seu-usuario/etl-meta-ads:latest

# Deploy no Swarm
docker stack deploy -c docker-compose.yml etl-meta
```

#### Opção B: CI/CD Automático (Recomendado)

1. Configure os secrets no GitHub:
   - `DOCKERHUB_USERNAME`
   - `DOCKERHUB_TOKEN`

2. Faça push para a branch `main`:

```bash
git add .
git commit -m "feat: initial ETL setup"
git push origin main
```

3. A GitHub Action fará o build e push automaticamente

4. Deploy no Swarm:

```bash
docker stack deploy -c docker-compose.yml etl-meta
```

## 📊 Métricas Coletadas

| Categoria       | Métricas                                                             |
| --------------- | -------------------------------------------------------------------- |
| **Básicas**     | impressões, cliques_saida, clique_link, valor_gasto                  |
| **Conversão**   | lp_view, lead, contato, conversas_iniciadas, novos_contatos_mensagem |
| **Engajamento** | seguidores_instagram, visitas_perfil                                 |
| **E-commerce**  | initiate_checkout, compras, valor_compra                             |
| **Vídeo**       | videoview_3s, videoview_50, videoview_75                             |
| **Dimensões**   | plataforma, posicionamento, data_registro                            |

## 🔍 Monitoramento

### Verificar logs do serviço:

```bash
docker service logs -f etl-meta_meta_etl_worker
```

### Verificar status:

```bash
docker service ps etl-meta_meta_etl_worker
```

### Verificar rate limit:

Os logs mostrarão automaticamente:

```
Rate limit - App: 45% | Account: 23%
```

## 🛠️ Troubleshooting

### Problema: Dados zerados em algumas métricas

**Causa**: Action types customizados ou eventos não configurados no Pixel

**Solução**:

1. Execute `python discovery.py`
2. Identifique os nomes técnicos reais (ex: `offsite_conversion.custom.123456`)
3. Ajuste o mapeamento em `main.py` na função `transform_data()`

### Problema: Rate limit atingido

**Sintoma**: Logs mostram `Rate limit próximo do teto: 95%`

**Solução**:

- O pipeline pausa automaticamente por 2 minutos
- Se persistir, aumente o intervalo de execução no `schedule.every(4).hours`

### Problema: Timeout na API

**Sintoma**: `requests.exceptions.Timeout`

**Solução**:

- Verifique conectividade com `graph.facebook.com`
- Reduza o período de extração (atualmente 2 meses)

### Problema: Tabela não existe

**Sintoma**: `relation "insights_meta_ads" does not exist`

**Solução**:

```bash
psql -h haproxy -U postgres -d relatorio_meta_ads -f schema.sql
```

## 📈 Performance

- **Volume típico**: ~10.000 registros/conta/mês
- **Tempo de execução**: 2-5 min para 2 contas (depende do volume)
- **Uso de memória**: ~200MB
- **Uso de CPU**: ~0.3 cores durante extração

## 🔒 Segurança

- ✅ Usuário não-root no container
- ✅ `.env` no `.gitignore`
- ✅ Resource limits no Swarm
- ✅ Health checks configurados
- ✅ Rollback automático em falhas

## 📝 Período de Coleta

Por padrão, coleta dados de:

- **Início**: 1º dia do mês anterior
- **Fim**: Data atual

Isso garante captura de **janelas de atribuição atrasadas** (até 28 dias).

## 🔄 Atualização do Código

```bash
# Edite seus arquivos
git add .
git commit -m "feat: sua descrição"
git push origin main

# A GitHub Action fará o build automático

# Atualize o serviço no Swarm
docker service update --image seu-usuario/etl-meta-ads:latest etl-meta_meta_etl_worker
```

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-metrica`)
3. Commit suas mudanças (`git commit -m 'feat: adiciona métrica X'`)
4. Push para a branch (`git push origin feature/nova-metrica`)
5. Abra um Pull Request

## 📄 Licença

MIT License - veja o arquivo LICENSE para detalhes

## 📞 Suporte

- **Documentação Meta Ads**: https://developers.facebook.com/docs/marketing-apis
- **Issues**: Abra uma issue neste repositório
- **Rate Limits**: https://developers.facebook.com/docs/marketing-api/insights

---

**Desenvolvido com ❤️ para análise de dados de marketing**
