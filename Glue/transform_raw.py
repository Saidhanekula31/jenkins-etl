import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load CSVs
customer_df = spark.read.option("header", "true").csv("s3://fintech-etl-raw-bucket/data/raw/customer.csv")
account_df = spark.read.option("header", "true").csv("s3://fintech-etl-raw-bucket/data/raw/account.csv")
transaction_df = spark.read.option("header", "true").csv("s3://fintech-etl-raw-bucket/data/raw/transaction.csv")

# Clean & Join
customer_df = customer_df.drop("email", "ssn_plain", "dob")
df = transaction_df.join(account_df, "account_id", "left") \
                   .join(customer_df, "customer_id", "left") \
                   .withColumn("amount_dollars", (transaction_df.amount_cents / 100)) \
                   .drop("amount_cents", "created_at", "updated_at")

# Filter for posted transactions
df = df.filter(df.status == "posted")

# Save as partitioned Parquet
df.write.partitionBy("account_type").parquet("s3://fintech-etl-processed-bucket/transactions/", mode="overwrite")

job.commit()