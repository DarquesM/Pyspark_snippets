from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql import Window
# Working with Palantir foundry
fm = get_foundry_manager()
#Create Spark context
sc = fm._sql_context

# Read dataset and check schema, optionnal second argument to choose branch
df = fm.read_dataset("/prepare_pivoted_incremental")
df.printSchema()
# Moving average is done using a window
# Define window
windowSpec = Window.partitionBy('flight_hash').orderBy('timestamp').rowsBetween(Window.currentRow, 2)
# Create new column : calculate mean using the specified window
df = df.withColumn("mean_FF1", F.mean(df['FF_1']).over(windowSpec))

# Detect the first average value above a given threshold (100)
t_start = df.select('timestamp')\
                    .filter(df.mean_FF1 > 100)\
                    .sort('timestamp')\
                    .first()
