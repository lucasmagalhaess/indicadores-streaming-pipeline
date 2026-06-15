"""
Producer - Indicadores Economicos Streaming Pipeline

Este script busca indicadores economicos da API do Banco Central
e envia cada registro como um evento separado para o Kinesis Data Stream.

Em uma arquitetura real, este producer rodaria continuamente -
aqui simulamos enviando o historico recente, registro por registro,
como se cada um fosse um evento chegando em tempo real.

Conceito chave: o producer NAO sabe quem vai consumir os dados.
Ele so publica no stream. Quem consome (Lambda) processa de forma
totalmente desacoplada.
"""

import boto3
import requests
import json
import time
from datetime import datetime, timezone

kinesis_client = boto3.client("kinesis", region_name="us-east-2")

STREAM_NAME = "indicadores-stream"

INDICADORES = {
    "11":  "Selic",
    "1":   "Dolar PTAX",
    "433": "IPCA",
    "12":  "CDI",
}


def get_serie(codigo, data_inicio="01/01/2026"):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
    params = {"formato": "json", "dataInicial": data_inicio}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def send_to_kinesis(record):
    """Envia um registro para o Kinesis Data Stream.

    PartitionKey define em qual shard o registro vai cair.
    Usamos o indicador como chave - assim registros do mesmo
    indicador tendem a ficar na mesma ordem (importante para
    series temporais).
    """
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(record, ensure_ascii=False),
        PartitionKey=record["indicador"]
    )


def produce():
    print(f"Enviando registros para o Kinesis Stream: {STREAM_NAME}\n")

    total_enviados = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    for codigo, nome in INDICADORES.items():
        print(f"Buscando {nome}...")
        dados = get_serie(codigo)

        for d in dados:
            record = {
                "codigo": codigo,
                "indicador": nome,
                "data": d["data"],
                "valor": d["valor"],
                "produced_at": now
            }
            send_to_kinesis(record)
            total_enviados += 1

        print(f"  {len(dados)} registros enviados para o stream")
        # pequena pausa para nao sobrecarregar o shard
        time.sleep(0.2)

    print(f"\nTotal enviado ao Kinesis: {total_enviados} registros")


if __name__ == "__main__":
    produce()
