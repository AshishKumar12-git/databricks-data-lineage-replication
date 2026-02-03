# Databricks notebook source
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType 

# COMMAND ----------

schema = StructType(
    [
        StructField('run_id',StringType(),False),
        StructField('dataset_name',StringType(),False),
        StructField('load_type',StringType(),False),
        StructField('Timestamp',TimestampType(),False),
        StructField('source_count',LongType(),True),
        StructField('target_count',LongType(),True),
        StructField('status',StringType(),False),
        StructField('error_message',StringType(),True)
    ]
)

# COMMAND ----------

audit_path = 'dbfs:/replication/audit/audit_tables'
empty_df = spark.createDataFrame([],schema)
empty_df.write.format('delta').mode('overwrite').save(audit_path)

# COMMAND ----------

# empty_df.display()