%run "../includes/Configuration"

%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import lit

from pyspark.sql.functions import current_timestamp, col

#Populates the data at runtime, with the value in the box above
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

#Cutover file has historic data
spark.read.json("/mnt/form1ka22/raw/incremental_load_data/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

%sql
SELECT raceId, count(1)
FROM  results_cutover
GROUP BY raceId
ORDER BY raceId DESC;

#Delta files here have only weekly data
spark.read.json("/mnt/form1ka22/raw/incremental_load_data/2021-03-28/results.json").createOrReplaceTempView("results_w1")

%sql
SELECT raceId, count(1)
FROM   results_w1
GROUP BY raceId
ORDER BY raceId DESC;

#Other delta file
spark.read.json("/mnt/form1ka22/raw/incremental_load_data/2021-04-18/results.json").createOrReplaceTempView("results_w2")

%sql
SELECT raceId, count(1)
FROM   results_w2
GROUP BY raceId
ORDER BY raceId DESC;

#Populates the data at runtime, with the value in the box above
dbutils.widgets.text("p_data_source","")
v_data_src = dbutils.widgets.get("p_data_source")

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('grid', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionText', StringType(), True),
                                    StructField('positionOrder', IntegerType(), True),
                                    StructField('points', FloatType(), True),
                                    StructField('laps', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('fastestLap', IntegerType(), True),
                                    StructField('rank', IntegerType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('fastestLapSpeed', FloatType(), True),
                                    StructField('statusId', StringType(), True),
                                    StructField("data_source", StringType(),False),
                                    StructField("file_date", StringType(), False)
                                    ])

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/incremental_load_data/{v_file_date}/results.json")\
    .withColumn("data_source", lit(v_data_src))\
    .withColumn("file_date", lit(v_file_date))

#Renaming columns and adding new
results_with_cols_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text")\
    .withColumn("inges_date", current_timestamp())

#Drop unwanted col
results_final_df = results_with_cols_df.drop(col("statusId"))

results_final_df.columns

#Be careful when doing a collect op, because it takes all the data and puts it inside the driver node's memory
#Only collect small amount of data
#Never do a collect on a normal dataframe!!
# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_proc.results")):
#        spark.sql(f"ALTER TABLE f1_proc.results DROP IF EXISTS PARTITION(race_id = {race_id_list.race_id})")

#results_final_df.write.mode('append').format("parquet").saveAsTable("f1_proc.results")

%sql
--DROP TABLE f1_proc.results;

overwrite_partition(results_final_df, 'f1_proc','results','race_id')

#results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/form1ka22/proc/results")

dbutils.notebook.exit("Success")

%sql
SELECT race_id,Count(1)
FROM f1_proc.results
GROUP BY race_id 
ORDER BY race_id DESC;



