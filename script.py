import os
import time
import logging
import requests

from pyspark.sql import SparkSession,HiveContext
from pyspark.sql import functions as f 
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import * 


def test():
	sparkdriver=SparkSession.builder.master('local').enableHiveSupport().getOrCreate()
	df_jdbc=sparkdriver.read.format('jdbc').option('url','jdbc:mysql://localhost:3306').option('user','root').option('password','hadoop').option('query','select * from kalyan.orders').load()
	print("data frame reading successful")
	df_jdbc.select("order_id","order_status").write.saveAsTable("kalyan.ooorders999")


print("done")
if __name__=="__main__":
	test()


print("well")



		
	