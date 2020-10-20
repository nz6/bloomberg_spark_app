import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

conf = pyspark.SparkConf().setAppName('bbg_credit_risk').setMaster('local')
sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

datafile = sc.textFile('path_to_the_file')

headerlist = datafile.take(500)
headerrdd = sc.parallelize(headerlist)
header = headerrdd.map(lambda line: line.split("|"))\
    .filter(lambda line: line[0].find('START-OF')<0 and line[0].find('END-OF')<0)\
    .filter(lambda line: len(line)==1 and len(line[0])>0 and line[0].find('=')<0 and line[0].find('#')<0)\
    .map(lambda line: line[0])\
    .collect()

header = ['ID', 'RETURN_CODE', 'NUMBER_OF_FIELDS'] + header + ['LAST_EMPTY_COLUMN']
fields = [StructField(field_name, StringType(), True)
      for field_name in header]

schema = StructType(fields)
data = datafile.map(lambda line: line.split("|")).filter(lambda line: len(line)>1)
data_df = sqlContext.createDataFrame(data, schema)
data_df.createOrReplaceTempView("CREDIT_RISK")
#data_df2 = spark.sql("select CR2.LONG_COMP_NAME as UltimateParent, \
#                    count(*) over(Partition by CR2.ID_BB_ULTIMATE_PARENT_CO) as Count \
#                    from  CREDIT_RISK CR1, CREDIT_RISK CR2 \
#                    where CR1.ID_BB_COMPANY=CR2.ID_BB_ULTIMATE_PARENT_CO \
#                    order by count desc")

data_df2 = spark.sql("select INDUSTRY_SECTOR Sector, count(*) as SectorCount \
                    from  CREDIT_RISK \
                    where trim(INDUSTRY_SECTOR) != '' \
                    group by INDUSTRY_SECTOR \
                    order by SectorCount DESC")

# Chart industry sectors
from math import pi
import pandas as pd
from bokeh.io import output_file, show
from bokeh.palettes import Category20c
from bokeh.plotting import figure
from bokeh.transform import cumsum

pie_data = data_df2.toPandas().reset_index()
pie_data['SectorCount'] = pie_data['SectorCount'].astype(int)
pie_data['SectorCount'] = round(pie_data['SectorCount']/pie_data['SectorCount'].sum(), 2) * 100
pie_data['angle'] = pie_data['SectorCount']/pie_data['SectorCount'].sum() * 2*pi
pie_data['color'] = Category20c[len(pie_data)]

p = figure(plot_width=700, title="Industry Sectors", toolbar_location=None,
        tools="hover", tooltips="@Sector: @SectorCount")

p.wedge(x=0, y=1, radius=0.4,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend='Sector', source=pie_data)

show(p)
