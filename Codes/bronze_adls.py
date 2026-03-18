# Databricks notebook source
import pandas as pd

df = pd.read_json(
        f"https://dldriverreallocation.blob.core.windows.net/raw/ingestion/"
        f"map_cities.json"
        f"?sp=r&st=2026-03-16T12:16:38Z&se=2026-04-14T20:31:38Z"
        f"&spr=https&sv=2024-11-04&sr=c&sig=ZIE7fM4ngZQy9o1vcqPoALDnugzOhRuLAe3RlwGTVCs%3D"
    )

df_spark = spark.createDataFrame(df)
display(df_spark)

# COMMAND ----------

import pandas as pd

files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"},
]
 
for file in files:
 
    url = (
        f"https://dldriverreallocation.blob.core.windows.net/raw/ingestion/"
        f"{file['file']}.json"
        f"?sp=r&st=2026-03-16T12:16:38Z&se=2026-04-14T20:31:38Z"
        f"&spr=https&sv=2024-11-04&sr=c&sig=ZIE7fM4ngZQy9o1vcqPoALDnugzOhRuLAe3RlwGTVCs%3D"
    )
 
    df       = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
 
    
    df_spark.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")\
            .saveAsTable(f"driverreallocation.bronze.{file['file']}")

# COMMAND ----------

url = (
        f"https://dldriverreallocation.blob.core.windows.net/raw/ingestion/"
        f"map_cities.json"
        f"?sp=r&st=2026-03-16T12:16:38Z&se=2026-04-14T20:31:38Z"
        f"&spr=https&sv=2024-11-04&sr=c&sig=ZIE7fM4ngZQy9o1vcqPoALDnugzOhRuLAe3RlwGTVCs%3D"
)

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)

if not spark.catalog.tableExists("uber.bronze.map_cities"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"driverreallocation.bronze.map_cities")
    print("This will not run more than 1 time")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM driverreallocation.bronze.map_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM driverreallocation.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM driverreallocation.bronze.rides_raw

# COMMAND ----------

