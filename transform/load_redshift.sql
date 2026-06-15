-- Cria a tabela gold no Redshift Serverless
CREATE TABLE IF NOT EXISTS indicadores_gold (
    codigo VARCHAR(10),
    indicador VARCHAR(50),
    data VARCHAR(20),
    valor_arredondado FLOAT,
    classificacao VARCHAR(20),
    ano INT,
    mes INT
);

-- Carrega os dados do S3 (silver) direto para o Redshift
-- COPY e o comando mais eficiente do Redshift para bulk load
COPY indicadores_gold
FROM 's3://indicadores-streaming-datalake-2026/silver/indicadores/'
IAM_ROLE default
FORMAT AS PARQUET;
