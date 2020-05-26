

path = "s3a://capstone-dend-dangkhoipham/raw/airport-codes_csv.csv"
df = spark.read.csv(path, header = True, inferSchema = True)
output = "s3://capstone-dend-dangkhoipham/transform/"
df.write.csv(output+"airport/", mode = "override")