# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "017d68f6-197f-4bd8-a06e-49a21a2c5d18",
# META       "default_lakehouse_name": "lh_tc2_alfabetizacao",
# META       "default_lakehouse_workspace_id": "b3909c63-42fa-4520-87ed-683ada991154",
# META       "known_lakehouses": [
# META         {
# META           "id": "017d68f6-197f-4bd8-a06e-49a21a2c5d18",
# META           "name": "lh_tc2_alfabetizacao"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run the below script or schedule it to run regularly to optimize your Lakehouse table 'bronze_streaming_simulado'

from delta.tables import *
deltaTable = DeltaTable.forName(spark, "bronze_streaming_simulado")
deltaTable.optimize().executeCompaction()

# If you only want to optimize a subset of your data, you can specify an optional partition predicate. For example:
#
#     from datetime import datetime, timedelta
#     startDate = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
#     deltaTable.optimize().where("date > '{}'".format(startDate)).executeCompaction()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
