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

# Script generated for node step_landing
step_landing_node1751654930764 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="step_trainer_landing", transformation_ctx="step_landing_node1751654930764")

# Script generated for node customer_curated
customer_curated_node1751654930217 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_curated", transformation_ctx="customer_curated_node1751654930217")

# Script generated for node SQL Query
SqlQuery292 = '''
SELECT trainer.*
FROM trainer
JOIN customers
ON trainer.serialNumber = customers.serialnumber
'''
SQLQuery_node1751655098504 = sparkSqlQuery(glueContext, query = SqlQuery292, mapping = {"trainer":step_landing_node1751654930764, "customers":customer_curated_node1751654930217}, transformation_ctx = "SQLQuery_node1751655098504")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1751655098504, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751654921263", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1751655144760 = glueContext.getSink(path="s3://stedi-project-data-cei/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1751655144760")
steptrainertrusted_node1751655144760.setCatalogInfo(catalogDatabase="stedi_project_db",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1751655144760.setFormat("json")
steptrainertrusted_node1751655144760.writeFrame(SQLQuery_node1751655098504)
job.commit()