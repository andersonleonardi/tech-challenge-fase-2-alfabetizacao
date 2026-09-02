# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install azure-eventhub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient, EventData
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONNECTION_STRING = "<CONFIGURAR_LOCALMENTE>"
EVENTHUB_NAME = "<CONFIGURAR_LOCALMENTE>"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eventos = [
    {
        "ano": 2025,
        "id_municipio": "1100015",
        "rede": "Municipal",
        "taxa_alfabetizacao": 70.25,
        "tipo_evento": "atualizacao_indicador"
    },
    {
        "ano": 2025,
        "id_municipio": "1100031",
        "rede": "Municipal",
        "taxa_alfabetizacao": 76.40,
        "tipo_evento": "atualizacao_indicador"
    },
    {
        "ano": 2025,
        "id_municipio": "1100080",
        "rede": "Municipal",
        "taxa_alfabetizacao": 63.10,
        "tipo_evento": "atualizacao_indicador"
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    eventhub_name=EVENTHUB_NAME
)

with producer:
    batch = producer.create_batch()

    for evento in eventos:
        batch.add(
            EventData(json.dumps(evento))
        )

    producer.send_batch(batch)

print(f"{len(eventos)} eventos enviados com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
