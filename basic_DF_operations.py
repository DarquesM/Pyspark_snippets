from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql import Window

fm = get_foundry_manager()
#Create Spark context
sc = fm._sql_context

a = sc.createDataFrame([(1, 1, 16), 
                        (2, 1, 100),
                        (3, 2, 150),
                        (4, 2, 150),
                        (5, 2, 6),
                        (6, 3, 99),
                        (7, 3, 101),
                        (8, 4, 103),
                        (9, 4, 12),
                        (10, 4, 18),
                        (11, 4, 47),
                        (12, 4, 44)]
                       , ['ID', 'Phase', 'FF_1'])

# Rolling average over 2 rows
window_a = Window.orderBy('Phase')\
                 .rowsBetween(0, 1)
a.withColumn("Result", F.mean(a['A']).over(window_a)).show()

# Compute mean of FF_1 for each phase value
a.groupby('Phase').agg({'FF_1': 'avg'}).show()

def phase_detection(df):
    det_phase = (F.lag(col('Phase'), 0).over(window_a)) !=\
                    (F.lead(col('Phase'), 1).over(window_a))
    #det_end = (F.last(col('Phase')).over(window_b)).isNull() 
    det_end = (F.lead(col('Phase'), 1).over(window_a)).isNull()    
    result_phase = (when(det_phase, df['Phase'])\
                   .when(det_end, df['Phase'])\
                   .otherwise(0))
    return result_phase

sum_cols = udf(lambda x: x[0]+x[1], IntegerType())
