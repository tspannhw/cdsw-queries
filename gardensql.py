from __future__ import print_function
import pandas as pd
import sys, re
from operator import add
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

plt.close('all')
  
pd.options.display.html.table_schema = True
  
spark = SparkSession\
  .builder\
  .appName("Visualize garden data")\
  .getOrCreate()

# Access the parquet  
garden = spark.read.parquet("/tmp/garden/*.parquet")
gdata = garden.toPandas()
df = pd.DataFrame(gdata)

df.plot()

df.plot.bar()

# Access the parquet  
gtf = spark.read.parquet("/tmp/gardentf/*.parquet")
gtfdata = gtf.toPandas()
pd.DataFrame(gtfdata)

spark.stop()
