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

# Script generated for node customer_landing
customer_landing_node1757170316261 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://myudacityglues3/customer/landing/customer-1691348231425.json"], "recurse": True}, transformation_ctx="customer_landing_node1757170316261")

# Script generated for node Share_with_research
SqlQuery819 = '''
select * from myDataSource
where myDataSource.sharewithresearchasofdate is not null;
'''
Share_with_research_node1757170443726 = sparkSqlQuery(glueContext, query = SqlQuery819, mapping = {"myDataSource":customer_landing_node1757170316261}, transformation_ctx = "Share_with_research_node1757170443726")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=Share_with_research_node1757170443726, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757170303322", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1757170604828 = glueContext.getSink(path="s3://myudacityglues3/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1757170604828")
customer_trusted_node1757170604828.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
customer_trusted_node1757170604828.setFormat("json")
customer_trusted_node1757170604828.writeFrame(Share_with_research_node1757170443726)
job.commit()