# Notebook as python script for automated execution

import pyspark
from delta import configure_spark_with_delta_pip
from deltalake import DeltaTable
import pandas as pd


#create spark context
builder = pyspark.sql.SparkSession.builder.appName("mlops") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# read data
df_test = spark.read.csv("test_FD001.txt", sep=" ")
df_train = spark.read.csv("train_FD001.txt", sep=" ")
df_rul = spark.read.csv("RUL_FD001.txt", sep=" ",)


# save data in delta lake bronze stage
df_test.write.format("delta").save("delta-lake/bronze/test_data")
df_train.write.format("delta").save("delta-lake/bronze/train_data")
df_rul.write.format("delta").save("delta-lake/bronze/rul_data")

# define column names
cols=["id","cycle","op1","op2","op3","sensor1","sensor2","sensor3","sensor4","sensor5","sensor6","sensor7","sensor8",
         "sensor9","sensor10","sensor11","sensor12","sensor13","sensor14","sensor15","sensor16","sensor17","sensor18","sensor19"
         ,"sensor20","sensor21","sensor22","sensor23"]

# read from deltalake
dt_test = DeltaTable("delta-lake/bronze/test_data")
dt_train = DeltaTable("delta-lake/bronze/train_data")
dt_rul =  DeltaTable("delta-lake/bronze/rul_data")

# convert to dataframe
test = dt_test.to_pandas()
train = dt_train.to_pandas()
test_results = dt_rul.to_pandas()

# set column names
test.columns = cols
train.columns = cols

# set datatypes
test = test.apply(pd.to_numeric)
train = train.apply(pd.to_numeric)

test_results = test_results.apply(pd.to_numeric)
test_results.columns=["rul", "null"]

test_results['id']=test_results.index+1
test_results.drop(["null"],axis=1,inplace=True)
rul = pd.DataFrame(test.groupby('id')['cycle'].max()).reset_index()
rul.columns = ['id', 'max']
test_results['rul_failed']=test_results['rul']+rul['max']
test_results.drop(["rul"],axis=1,inplace=True)
test=test.merge(test_results,on=['id'],how='left')
test["remaining_cycle"]=test["rul_failed"]-test["cycle"]

df_train=train.drop(["sensor22","sensor23"],axis=1)
df_test=test.drop(["sensor22","sensor23"],axis=1)
df_test.drop(["rul_failed"],axis=1,inplace=True)
df_train['remaining_cycle'] = df_train.groupby(['id'])['cycle'].transform(max)-df_train['cycle']

# set CYCLES variable to predict if the engine will fail within the next n cycles
CYCLES = 20 # predict if the engine will fail in the next 20 cycles
df_train['label'] = df_train['remaining_cycle'].apply(lambda x: 1 if x <= CYCLES else 0)
df_test['label'] = df_test['remaining_cycle'].apply(lambda x: 1 if x <= CYCLES else 0)

df_train["sensor17"] = pd.to_numeric(df_train["sensor17"], downcast="float")
df_test["sensor17"] = pd.to_numeric(df_test["sensor17"], downcast="float")
df_train["sensor18"] = pd.to_numeric(df_train["sensor18"], downcast="float")
df_test["sensor18"] = pd.to_numeric(df_test["sensor18"], downcast="float")
df_train["sensor19"] = pd.to_numeric(df_train["sensor19"], downcast="float")
df_test["sensor19"] = pd.to_numeric(df_test["sensor19"], downcast="float")

# convert pandas dataframes to spark dataframes
df_test_silver = spark.createDataFrame(df_test)
df_train_silver = spark.createDataFrame(df_train)

# save prepared data in delta lake silver stage
df_test_silver.write.format("delta").save("delta-lake/silver/test_data")
df_train_silver.write.format("delta").save("delta-lake/silver/train_data")
