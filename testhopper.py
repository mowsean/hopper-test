# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types
import pyspark.sql.utils
from datetime import datetime, timedelta, date

from  pyspark.sql.types import StructType, StructField, TimestampType, StringType, MapType, ArrayType, TimestampType
from pyspark.sql.window import Window
import pytz

runDebugCommands = True

tzLA = pytz.timezone('America/Los_Angeles') 

# COMMAND ----------

# MAGIC %run "/Managed Shares/_Libraries/Hopper Helper and Common Code"

# COMMAND ----------

HH = HopperHelper()

# COMMAND ----------

sFilter = "MachineName='TKDDEVMOHSIN'"
sDate = datetime(2022,8,10)
eDate = datetime(2022,8,25)
print("test")

# COMMAND ----------

# use this line below for specific dates
dfRawEvents = HH.loadData(dataSource=HopperDataSource.Env08_RawEventJson, startDate=sDate, endDate=eDate, optionalFilterString="")
dfEventsFiltered = dfRawEvents.filter(sFilter).orderBy(F.desc("UtcTimeStamp"))
dfEventsFiltered = dfEventsFiltered.withColumn("TemplateTypeCode",F.col("Details.Parameters.TemplateTypeCode"))


rawCount = dfRawEvents.count()
inputRecordCount = dfEventsFiltered.count()
print("Raw Counts: " + "{:,}".format(rawCount))
print("Counts: " + "{:,}".format(inputRecordCount))

# COMMAND ----------

dfGrouped = dfEventsFiltered.groupBy("TemplateTypeCode").count()\
  .withColumn('total', F.sum('count').over(Window.partitionBy()))\
  .withColumn('percent', F.round(F.col('count')/F.col('total')*100,1))

display(dfGrouped.orderBy(F.desc("count")))
