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

# Script generated for node accelerometer
accelerometer_node1751599244224 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="accelerometer_landing", transformation_ctx="accelerometer_node1751599244224")

# Script generated for node customers
customers_node1751599243506 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_trusted", transformation_ctx="customers_node1751599243506")

# Script generated for node SQL Query - Accelerator customers
SqlQuery339 = '''
SELECT accelerometer.*
FROM accelerometer
JOIN customers
ON accelerometer.user = customers.email;
'''
SQLQueryAcceleratorcustomers_node1751599348682 = sparkSqlQuery(glueContext, query = SqlQuery339, mapping = {"customers":customers_node1751599243506, "accelerometer":accelerometer_node1751599244224}, transformation_ctx = "SQLQueryAcceleratorcustomers_node1751599348682")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQueryAcceleratorcustomers_node1751599348682, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751599713023", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1751599759176 = glueContext.getSink(path="s3://stedi-project-data-cei/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1751599759176")
accelerometer_trusted_node1751599759176.setCatalogInfo(catalogDatabase="stedi_project_db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1751599759176.setFormat("json")
accelerometer_trusted_node1751599759176.writeFrame(SQLQueryAcceleratorcustomers_node1751599348682)
job.commit()