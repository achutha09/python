
from pyspark.sql.functions import substring
from pyspark.sql.functions import  when, lit, asc

uri = "abfss://assign1@achuthastorage.dfs.core.windows.net/"
storage_end_point = "achuthastorage.dfs.core.windows.net"
my_scope = "databricksSecretScope"
my_key = "storage-keyvault"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

dat_df = spark.read.option("delimiter", "|").csv(uri+"DailyMeterData.dat", header=True)
# display(dat_df)
csvDataFile = spark.read.csv(uri+'CustMeter.csv', header=True)

common_columns = ['Customer Account Number', 'Meter Number', 'ServiceType', 'DT', 'Serial Number', 'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time']

QC_columns = ["QC#1", "QC#2", "QC#3", "QC#4", "QC#5", "QC#6", "QC#7", "QC#8", "QC#9", "QC#10","QC#11", "QC#12", "QC#13", "QC#14", "QC#15", "QC#16", "QC#17", "QC#18", "QC#19", "QC#20"
    ,"QC#21", "QC#22", "QC#23", "QC#24"]

Interval_columns = ["Interval#1", "Interval#2", "Interval#3", "Interval#4", "Interval#5", "Interval#6", "Interval#7", "Interval#8", "Interval#9", "Interval#10",
                    "Interval#11", "Interval#12", "Interval#13", "Interval#14", "Interval#15", "Interval#16", "Interval#17", "Interval#18", "Interval#19", "Interval#20",
                    "Interval#21", "Interval#22", "Interval#23", "Interval#24"]


def melt_csv(csvDataFile,dat_df):
        global joined_df
        dat_df_csv_datafile = csvDataFile.join(dat_df.drop("Meter Number"), on="Customer Account Number", how="inner")
        dataframe_pivot1 = dat_df_csv_datafile.unpivot(common_columns, QC_columns, "IntervalHour", "QCCode")
        # slice first 3 characters of "IntervalHour" column to get number
        dataframe_pivot2 = dat_df_csv_datafile.unpivot(common_columns, Interval_columns, "IntervalHour",
                                                       "IntervalValue")
        # slice first 9 characters of "IntervalHour" column to get number
        df1 = dataframe_pivot2.withColumn("IntervalHour", substring("IntervalHour", 10, 2).cast("int"))
        df2 = dataframe_pivot1.withColumn("IntervalHour", substring("IntervalHour", 4, 2).cast("int"))
        joined_df = df1.join(df2, on=['Customer Account Number', 'Meter Number', 'ServiceType', 'DT', 'Serial Number',
                                      'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time',
                                      "IntervalHour", ], how="inner")


melt_csv(csvDataFile,dat_df)

noduplicates=joined_df.dropDuplicates();
df_filtered = noduplicates.filter((noduplicates["Data Type"] != "Reverse Energy in Wh") & (noduplicates["Data Type"] != "Net Energy in WH") & (noduplicates["QCCode"] == 3))
df_filtered_sorted = df_filtered.orderBy(
    asc("Customer Account Number"),
    asc("Data Type"),
    asc("Meter Number"),
    asc("Start Date"),
    asc("IntervalHour")
)

#display(df_filtered_sorted)
df_filtered_sorted.write.mode('overwrite').parquet("abfss://assign1@achuthastorage.dfs.core.windows.net/output/Paraquet")
num_partitions = 1
df_filtered_sorted = df_filtered_sorted.repartition(num_partitions)
# Write the DataFrame to Parquet format
df_filtered_sorted.write.mode('overwrite').parquet("abfss://assign1@achuthastorage.dfs.core.windows.net/output/partition/Paraquet")

# csv_data = df_filtered_sorted.toPandas().to_csv(index=False, header=True)
df_filtered_sorted.coalesce(1).write.option('header', True).mode('overwrite').parquet(uri+"output/coalesce/Paraquet")
df_filtered_sorted.coalesce(1).write.option('header', True).mode('overwrite').csv(uri+"output/CSV")

para_data = spark.read.parquet("abfss://assign1@achuthastorage.dfs.core.windows.net/output/partition/Paraquet")

DF = para_data.agg({"IntervalValue": "sum"}).collect()
DF1 = para_data.filter(para_data["ServiceType"] == "Residential").agg({"IntervalValue": "sum"})
DF2 = para_data.filter(para_data["IntervalHour"] == "7").agg({"IntervalValue": "sum"})
display(DF)
display(DF1)
display(DF2)