-- Schema para ETL Meta Ads
-- Execute este script no PostgreSQL antes de rodar o pipeline

CREATE TABLE IF NOT EXISTS insights_meta_ads (
    -- Identificadores
    account_id VARCHAR(50) NOT NULL,
    nome_conta VARCHAR(255),
    id_campanha BIGINT NOT NULL,
    id_conjunto_anuncios BIGINT NOT NULL,
    id_anuncio BIGINT NOT NULL,
    
    -- Nomes (podem ser atualizados)
    campanha VARCHAR(255),
    conjunto_anuncios VARCHAR(255),
    anuncio VARCHAR(255),
    
    -- Métricas principais
    impressoes BIGINT DEFAULT 0,
    cliques_saida INTEGER DEFAULT 0,
    clique_link INTEGER DEFAULT 0,
    
    -- Métricas de conversão
    lp_view INTEGER DEFAULT 0,
    lead INTEGER DEFAULT 0,
    contato INTEGER DEFAULT 0,
    conversas_iniciadas INTEGER DEFAULT 0,
    novos_contatos_mensagem INTEGER DEFAULT 0,
    
    -- Métricas de engajamento
    seguidores_instagram INTEGER DEFAULT 0,
    visitas_perfil INTEGER DEFAULT 0,
    
    -- Métricas de e-commerce
    initiate_checkout INTEGER DEFAULT 0,
    compras INTEGER DEFAULT 0,
    valor_compra NUMERIC(12, 2) DEFAULT 0,
    
    -- Métricas de vídeo
    videoview_3s INTEGER DEFAULT 0,
    videoview_50 INTEGER DEFAULT 0,
    videoview_75 INTEGER DEFAULT 0,
    
    -- Dimensões
    data_registro DATE NOT NULL,
    plataforma VARCHAR(50),
    posicionamento VARCHAR(100),
    
    -- Custos
    valor_gasto NUMERIC(12, 2) DEFAULT 0,
    
    -- Timestamps de controle
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_account_date ON insights_meta_ads(account_id, data_registro);
CREATE INDEX IF NOT EXISTS idx_campaign ON insights_meta_ads(id_campanha);
CREATE INDEX IF NOT EXISTS idx_adset ON insights_meta_ads(id_conjunto_anuncios);
CREATE INDEX IF NOT EXISTS idx_ad ON insights_meta_ads(id_anuncio);
CREATE INDEX IF NOT EXISTS idx_data_registro ON insights_meta_ads(data_registro);

-- Comentários para documentação
COMMENT ON TABLE insights_meta_ads IS 'Dados de insights da API Meta Ads com janela de atribuição de 28 dias';
COMMENT ON COLUMN insights_meta_ads.account_id IS 'ID da conta de anúncios (formato: act_123456789)';
COMMENT ON COLUMN insights_meta_ads.data_registro IS 'Data do registro reportado pela API';
COMMENT ON COLUMN insights_meta_ads.valor_gasto IS 'Valor gasto em USD (ou moeda da conta)';
