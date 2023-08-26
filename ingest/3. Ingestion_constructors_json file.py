from pyspark.sql.functions import lit

%run "../includes/Configuration"
#Populates the data at runtime, with the value in the box above

dbutils.widgets.text("p_data_source","")
v_data_src = dbutils.widgets.get("p_data_source")

#Populates the data at runtime, with the value in the box above
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

#Read the Json file using spark read
#Different way for defining a schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING, data_source STRING, file_date string"

cons_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/incremental_load_data/{v_file_date}/constructors.json")\
    .withColumn("data_source", lit(v_data_src))\
    .withColumn("file_date", lit(v_file_date))

display(cons_df)

cons_df.printSchema()

#Drop url
cons_df_no_url = cons_df.drop('url')

#A better way to do this when two dataframes are joined and have similar col names
#cons_df_no_url = cons_df.drop(cons_df.url)
#from pyspark.sql.functions import col
#cons_df_no_url = cons_df.drop(col('url')

from pyspark.sql.functions import current_timestamp

#Rename cols and add ingestion date
cons_final_df = cons_df_no_url.withColumnRenamed("contructorId", "cons_Id")\
                               .withColumnRenamed("constructorRef" , "cons_ref")\
                                .withColumn("ingestion_date", current_timestamp())

display(cons_final_df)

cons_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_proc.constructors")

#Write output to parquet file
cons_final_df.write.mode('overwrite').parquet("/mnt/form1ka22/proc/constructors")

%sql 
SELECT * FROM f1_proc.constructors

dbutils.notebook.exit("Success")
