from pyspark.sql import SparkSession 
  
spark = SparkSession.builder.appName("DataFrame").getOrCreate() 
  
# df = spark.read.text("output.txt") 
  
# df.selectExpr("split(value, ' ') as\ 
# Text_Data_In_Rows_Using_Text").show(4,False)




textFile = spark.read.text("README.md")
textFile.count()  # Number of rows in this DataFrame
textFile.first()  # First row in this DataFrame
# Row(value=u'# Apache Spark')