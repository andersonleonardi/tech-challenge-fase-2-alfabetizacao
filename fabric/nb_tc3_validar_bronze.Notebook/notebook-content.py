# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffd90b59-5acd-4630-94dc-e5284df5f3cc",
# META       "default_lakehouse_name": "lh_tc3_alfabetizacao",
# META       "default_lakehouse_workspace_id": "b3909c63-42fa-4520-87ed-683ada991154",
# META       "known_lakehouses": [
# META         {
# META           "id": "ffd90b59-5acd-4630-94dc-e5284df5f3cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import hashlib

caminho = (
    "/lakehouse/default/Files/bronze/"
    "censo_escolar_2025/2025_Tabela_Escola_V2.csv"
)

hash_sha256 = hashlib.sha256()

with open(caminho, "rb") as arquivo:
    for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
        hash_sha256.update(bloco)

hash_fabric = hash_sha256.hexdigest()

hash_local = (
    "f737d25dc5e80879e08f868d06d55f2bc1178cbb893dc14589a2123151f5b523"
)

print(f"SHA-256 Fabric: {hash_fabric}")
print(f"SHA-256 Local:  {hash_local}")
print(f"Integridade: {'VALIDADA' if hash_fabric == hash_local else 'DIVERGENTE'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
