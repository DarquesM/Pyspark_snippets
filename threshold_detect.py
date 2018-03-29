'''
Parcourir un spark DataFrame et détecter quand une entrée depasse une 
valeur seuil.
Les conditions :
    * FF_1 est supérieur à 100 et la valeur précédente inférieure à 100
    * FF_1 est inférieur à 100 et la valeur précédente supérieure à 100
    
NB:
Il faut utiliser 0 comme paramètre dans lead pour det_start
et dans lag pour det_stop pour ne pas prendre en compte la ligne
précédente ou bien la suivante, et fausser la détection
'''

# Import when apart from other functions otherwise using F.when() does not work
from pyspark.sql.functions import when
from pyspark.sql import functions as F

a = sc.createDataFrame([(1, 1, 16), 
                        (2, 4, 100),
                        (3, 5, 150),
                        (4, 45, 150),
                        (5, 12, 6),
                        (6, 50, 99),
                        (7, 50, 101),
                        (8, 50, 103)]
                       , ['ID', 'A', 'FF_1'])
a.show()

# Define window, no need to specify range (will be done via .lag() and .lead()
window_a = Window.orderBy('ID')                 

det_start = (F.lag(col('FF_1')).over(window_a) < 100)\
          & (F.lead(col('FF_1'), 0).over(window_a) >= 100)
det_end = (F.lag(col('FF_1'), 0).over(window_a) > 100)\
        & (F.lead(col('FF_1')).over(window_a) < 100)

result = (when(det_start, 1)\
           .when(det_end, 2)\
           .otherwise(0))

a.withColumn("res1", result).show()
