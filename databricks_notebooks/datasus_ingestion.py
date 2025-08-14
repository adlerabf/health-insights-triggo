# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Ingestion
# MAGIC For this step I've considered using the PySus library to download and handle the DBC files. 
# MAGIC This fastens the process of download files and auto-converts the files to parquet as a built-in process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Required Libraries
# MAGIC This implementation utilizes the PySus library to efficiently download and process DBC (DataSUS compressed) files. The library streamlines the data acquisition workflow by providing built-in functionality for file retrieval and automatic conversion to Parquet format, optimizing both download performance and storage efficiency.

# COMMAND ----------

!pip install pysus ibge simple_icd_10

# COMMAND ----------

import pandas as pd
from pysus import SIH
from collections import defaultdict
from pyspark.sql.functions import substring
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from ibge.localidades import *
import simple_icd_10 as icd

# COMMAND ----------

sih = SIH().load() # Loads the files from DATASUS

# COMMAND ----------

# by creating variables to my get_files function it becomes easy to manipulate wich files I want to download
state = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG",
    "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR",
    "RS", "SC", "SE", "SP", "TO"]
year = [2025]
month = [1,2,3,4,5,6] # For future implementations, I can add a function to automatically retrieve the latest monthly data releases

files = sih.get_files("RD", uf=state, year=year, month=month)
files

# COMMAND ----------

# Created a check-in looping to verify missing states
found_by_month = defaultdict(set)
for f in files:
    fname = f.name
    uf = fname[2:4]
    year_month = fname[4:6] + fname[6:8]
    month_num = int(fname[6:8])
    found_by_month[month_num].add(uf)

for m in month:
    found_states = found_by_month.get(m, set())
    missing_states = [uf for uf in state if uf not in found_states]
    if missing_states:
        print(f"Month {m:02d}: missing states: {missing_states}")
    else:
        print(f"Month {m:02d}: all states found.")

print(f'{len(state)} states were requested for each month.')

# COMMAND ----------

# Download the files and save them to a Databricks Volume 
sih.download(files, local_dir='/Volumes/triggo_project/datasus/src_datasus')

# COMMAND ----------

raw_df = spark.read.parquet("/Volumes/triggo_project/datasus/src_datasus/**/*.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating stg_datasus
# MAGIC Saving the raw_df parquet files as an unique partitioned parquet file in stg_datasus
# MAGIC

# COMMAND ----------


raw_df.write.mode("overwrite").parquet("/Volumes/triggo_project/datasus/stg_datasus")

# COMMAND ----------

stg_df = spark.read.parquet("/Volumes/triggo_project/datasus/stg_datasus")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding The UF_ZI_PREFIX Column
# MAGIC
# MAGIC Extracted the first two digits from UF_ZI column to create UF_ZI_PREFIX column, as these digits represent IBGE state codes.

# COMMAND ----------

stg_df = stg_df.withColumn("UF_ZI_PREFIX", substring("UF_ZI", 1, 2))
cols = ["UF_ZI_PREFIX"] + [col for col in stg_df.columns if col != "UF_ZI_PREFIX"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding the UF_CODE Column

# COMMAND ----------

# IBGE UF code mapping: using dictionary, replace with full mapping as needed
ibge_uf_map = {
    12: 'AC', 27: 'AL', 13: 'AM', 16: 'AP', 29: 'BA', 23: 'CE', 53: 'stg_df', 32: 'ES',
    52: 'GO', 21: 'MA', 31: 'MG', 50: 'MS', 51: 'MT', 15: 'PA', 25: 'PB', 26: 'PE',
    22: 'PI', 41: 'PR', 33: 'RJ', 24: 'RN', 43: 'RS', 11: 'RO', 14: 'RR', 42: 'SC',
    28: 'SE', 35: 'SP', 17: 'TO'
}


# Ustg_df to map UF_ZI_PREFIX to UF code
def get_uf_code(prefix):
    return ibge_uf_map.get(int(prefix), None)

get_uf_code_udf = udf(get_uf_code, StringType())

stg_df = stg_df.withColumn("UF_CODE", get_uf_code_udf("UF_ZI_PREFIX"))
display(stg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwriting stg_datasus with stg_df

# COMMAND ----------

stg_df.write.mode("overwrite").parquet("/Volumes/triggo_project/datasus/stg_datasus")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwriting S3 Bucket

# COMMAND ----------

stg_df.write.mode("overwrite").parquet("s3://datasus-sih/sih_data_parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the cities_data
# MAGIC The DATASUS dataset provides municipal identification through numeric codes: the 'MUNIC_RES' column contains the patient's municipality of residence, while the 'MUNIC_MOV' column indicates the healthcare establishment's municipality.

# COMMAND ----------

cities = Municipios()

# COMMAND ----------

cities_df = pd.DataFrame(cities.json())

# COMMAND ----------

display(cities_df)

# COMMAND ----------

cities_df = pd.DataFrame(cities.json())
display(spark.createDataFrame(cities_df))

# COMMAND ----------

cities_df_renamed = cities_df.rename(columns={'id': 'city_id', 'nome': 'city_name'})
spark_cities_df = spark.createDataFrame(cities_df_renamed)
spark_cities_df.write.mode("overwrite").parquet("/Volumes/triggo_project/datasus/docs/cities_data")

# COMMAND ----------

display(cities_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Cities Data in Volumes and S3 Bucket

# COMMAND ----------

dbutils.fs.cp("/Volumes/triggo_project/datasus/docs/cities_data/", "s3://datasus-sih/docs/cities_data", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ICD Data
# MAGIC The simple_icd_10 library was utilized to extract ICD data from the [World Health Organization](https://icd.who.int/browse10/2019/en#), enabling the creation of comprehensive ICD documentation.

# COMMAND ----------

codes = icd.get_all_codes(with_dots=False) 

rows = []
for c in codes:
    try:
        desc = icd.get_description(c)
    except ValueError:
        desc = None
    rows.append({"code": c, "description": desc})

df_icd = spark.createDataFrame(rows)
df_icd.write.mode("overwrite").csv("/Volumes/triggo_project/datasus/docs/icd_data")
dbutils.fs.cp(
    "/Volumes/triggo_project/datasus/docs/icd_data.csv",
    "s3://datasus-sih/docs/icd_data.csv"
)
