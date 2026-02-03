# Databricks notebook source
import json
import uuid
from datetime import datetime
from pyspark.sql.functions import col, max as spark_max
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./create_audit_tables

# COMMAND ----------

metadata_path = '/dbfs/FileStore/metadata/customer_delta.json'
with open(metadata_path,'r') as f:
    metadata = json.load(f)
dataset_name = metadata['dataset_name']
source_type = metadata['source']['type']
source_path = metadata['source']['path']
target_path = metadata['target']['path']
load_type = metadata['load_strategy']['type']
incremental_column = metadata['load_strategy']['incremental_column']
initial_value = metadata['load_strategy']['initial_value']
primary_keys = metadata['primary_key']

# COMMAND ----------

run_id = str(uuid.uuid4())
timestamp = datetime.now()

try:
    if source_type == 'delta':
        source_df = spark.read.format('delta').load(source_path)
    else:
        raise Exception(f"Unsuppoted source type : {source_type}")
    
    try:
        target_df = spark.read.format('delta').load(target_path)
        last_watermark = target_df.select(spark_max(col(incremental_column)).collect()[0][0])
        if last_watermark is None:
            last_watermark = initial_value
    except:
        last_watermark = initial_value

    incremental_df = source_df.filter(col(incremental_column) > last_watermark)
    incremental_count = incremental_df.count()
    if incremental_count > 0:
        if DeltaTable.isDeltaTable(spark,target_path):
            target_delta = DeltaTable.forPath(spark,target_path)
            merge_condition = "AND".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
            target_delta.alias('t').merge(incremental_df.alias('s'),merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            incremental_df.write.format('delta').save(target_path)

    source_count = incremental_count
    target_count = spark.read.format('delta').load(target_path).count()
    if last_watermark == initial_value:
        if source_count != target_count:
            raise Exception("rows are not matching")
    status = 'Success'
    error_message = None
    spark.createDataFrame([(run_id,dataset_name,load_type,timestamp,source_count,target_count,status,error_message)],schema).write.format('delta').mode('append').save(audit_path)

except Exception as e:
    status = 'Failed'
    error_message = str(e)
    spark.createDataFrame([(run_id,dataset_name,load_type,timestamp,None,None,status,error_message)],schema).write.format('delta').mode('append').save(audit_path)
    raise






# COMMAND ----------

df = spark.read.format('delta').load(audit_path)
display(df)