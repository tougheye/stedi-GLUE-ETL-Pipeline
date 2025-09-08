import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_curated
customer_curated_node1757252706250 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://myudacityglues3/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1757252706250")

# Script generated for node step_trainer_landing
step_trainer_landing_node1757252703709 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://myudacityglues3/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1757252703709")

# Script generated for node SQL Query
SqlQuery898 = '''
select * from stl
inner join cc
on stl.serialnumber = cc.serialnumber;
'''
SQLQuery_node1757252833313 = sparkSqlQuery(glueContext, query = SqlQuery898, mapping = {"cc":customer_curated_node1757252706250, "stl":step_trainer_landing_node1757252703709}, transformation_ctx = "SQLQuery_node1757252833313")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757252833313, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757251049254", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1757252895095 = glueContext.getSink(path="s3://myudacityglues3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757252895095")
AmazonS3_node1757252895095.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
AmazonS3_node1757252895095.setFormat("json")
AmazonS3_node1757252895095.writeFrame(SQLQuery_node1757252833313)
job.commit()