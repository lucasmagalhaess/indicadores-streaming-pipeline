"""
Lambda Consumer - Indicadores Economicos Streaming Pipeline

Esta funcao e triggerada automaticamente pelo Kinesis Data Stream
toda vez que novos registros chegam.

Fluxo:
1. Kinesis envia um batch de registros (encoded em base64)
2. Decodifica cada registro
3. Salva no S3 na camada bronze, particionado por data

Esse e o padrao "consumer" de uma arquitetura de streaming -
processa os dados conforme eles chegam, sem esperar um job batch.
"""

import json
import base64
import boto3
import os
from datetime import datetime, timezone

s3_client = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME")


def lambda_handler(event, context):
    records = event.get("Records", [])
    print(f"Recebidos {len(records)} registros do Kinesis")

    processed = []

    for record in records:
        # O Kinesis envia o dado codificado em base64
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        processed.append(data)

    if not processed:
        return {"statusCode": 200, "body": "Nenhum registro para processar"}

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S-%f")

    # Salva no bronze, particionado por data (boa pratica de Data Lake)
    key = f"bronze/indicadores/data={today}/registros_{timestamp}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(processed, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )

    print(f"Salvos {len(processed)} registros em s3://{BUCKET_NAME}/{key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "registros_processados": len(processed),
            "s3_key": key
        })
    }
