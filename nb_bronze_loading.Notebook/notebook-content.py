# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "816baf1d-ce43-49b9-b16a-07c9169f7772",
# META       "default_lakehouse_name": "lh_meta",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "816baf1d-ce43-49b9-b16a-07c9169f7772"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_id=""
pipeline_name=""
layer=""
source_name=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys
# sys.path.append('lh_meta/Files/py_files/')

print(sys.path)

from logging_utils import utils

time = utils.log_pipeline_start(spark, "12345", "test", "lol")
print(time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
