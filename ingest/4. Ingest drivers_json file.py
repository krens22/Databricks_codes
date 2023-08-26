from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

%run "../includes/Configuration"

#Populates the data at runtime, with the value in the box above
dbutils.widgets.text("p_data_source","")
v_data_src = dbutils.widgets.get("p_data_source")

#Populates the data at runtime, with the value in the box above
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

#Read the json file
#The json file has inner objects, so they need to be defined first

#Inner object
name_schema  = StructType(fields = [StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
                                    ])

#Outer object
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("dob", DateType(), True),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True),
                                 StructField("data_source", StringType(), False),
                                 StructField("file_date", StringType(), False)
                                 ])

driver_df = spark.read.\
option("header", True).\
schema(drivers_schema).\
json(f"{raw_folder_path}/incremental_load_data/{v_file_date}/drivers.json")\
    .withColumn("data_source", lit(v_data_src))\
    .withColumn("file_date", lit(v_file_date))

driver_df.printSchema()
display(driver_df)

drivers_withcols_df = driver_df.withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("name", concat(col('name.forename'), lit(" "), col('name.surname')))

display(drivers_withcols_df)

#Drop unwanted columns
drivers_final = drivers_withcols_df.drop(col('url'))

drivers_final.write.mode('overwrite').format("parquet").saveAsTable("f1_proc.drivers")

#Creates a new drivers folder
drivers_final.write.mode('overwrite').parquet("/mnt/form1ka22/proc/drivers")

display(spark.read.parquet("/mnt/form1ka22/proc/drivers"))

%sql
SELECT * FROM f1_proc.drivers

dbutils.notebook.exit("Success")
