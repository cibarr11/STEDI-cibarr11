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

# Script generated for node accel
accel_node1751645264010 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="accelerometer_trusted", transformation_ctx="accel_node1751645264010")

# Script generated for node customers
customers_node1751645263820 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_trusted", transformation_ctx="customers_node1751645263820")

# Script generated for node SQL Query
SqlQuery233 = '''
SELECT DISTINCT customers.*
FROM customers
JOIN accel
ON customers.email = accel.user
'''
SQLQuery_node1751645328283 = sparkSqlQuery(glueContext, query = SqlQuery233, mapping = {"accel":accel_node1751645264010, "customers":customers_node1751645263820}, transformation_ctx = "SQLQuery_node1751645328283")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1751645328283, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751644438782", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1751645398628 = glueContext.getSink(path="s3://stedi-project-data-cei/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1751645398628")
customer_curated_node1751645398628.setCatalogInfo(catalogDatabase="stedi_project_db",catalogTableName="customer_curated")
customer_curated_node1751645398628.setFormat("json")
customer_curated_node1751645398628.writeFrame(SQLQuery_node1751645328283)
job.commit()