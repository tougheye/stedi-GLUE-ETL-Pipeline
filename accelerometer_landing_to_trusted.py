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

# Script generated for node Customer_trusted
Customer_trusted_node1757172920824 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://myudacityglues3/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_trusted_node1757172920824")

# Script generated for node accelerometer_landing
accelerometer_landing_node1757172989692 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://myudacityglues3/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1757172989692")

# Script generated for node join_and_filter
SqlQuery947 = '''
select * from accl inner join ct on 
accl.user = ct.email
where accl.timestamp > ct.sharewithresearchasofdate;
'''
join_and_filter_node1757173090176 = sparkSqlQuery(glueContext, query = SqlQuery947, mapping = {"accl":accelerometer_landing_node1757172989692, "ct":Customer_trusted_node1757172920824}, transformation_ctx = "join_and_filter_node1757173090176")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=join_and_filter_node1757173090176, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757171842929", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1757173389197 = glueContext.getSink(path="s3://myudacityglues3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1757173389197")
accelerometer_trusted_node1757173389197.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1757173389197.setFormat("json")
accelerometer_trusted_node1757173389197.writeFrame(join_and_filter_node1757173090176)
job.commit()