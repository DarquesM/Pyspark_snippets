# Basic usage of user defined functions with pyspark
# These simple operations would normally not need an udf (just as an example)

from pyspark.sql import functions as F

# The x[0] argument applies to the F.struct object passed when calling the udf
# NOT to the DataFrame
# Although the F.struct gives 2 columns, only one can be used in the function

# When dividing, do not forget the dot after 1000 to comply with DoubleType()
div_mille_udf = F.udf(lambda x: x[0]/1000., DoubleType())
# Use F.struct instead of Array
df.withColumn('result', div_mille_udf(F.struct('timestamp', 'FF_1')))

# Sums columns of a DataFrame
sum_cols = udf(lambda x: x[0]+x[1], IntegerType())
# Depending on the context used, may also be pyspark.createDataFrame
a = sc.createDataFrame([(101, 1, 16)], ['ID', 'A', 'B'])
# Create 'Result' column.
a.withColumn('Result', sum_cols(F.struct('A', 'B'))).show()

# See : https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.withColumn
# Also : https://stackoverflow.com/questions/42540169/pyspark-pass-multiple-columns-in-udf
