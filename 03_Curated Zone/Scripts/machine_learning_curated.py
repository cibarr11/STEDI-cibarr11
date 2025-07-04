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
customer_curated_node1751657404929 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_curated", transformation_ctx="customer_curated_node1751657404929")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1751655401806 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1751655401806")

# Script generated for node acceletometer_trusted
acceletometer_trusted_node1751655402374 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="accelerometer_trusted", transformation_ctx="acceletometer_trusted_node1751655402374")

# Script generated for node SQL Query
SqlQuery316 = '''
SELECT
  trainer.sensorReadingTime,
  trainer.serialNumber,
  trainer.distanceFromObject,
  accel.user,
  accel.x,
  accel.y,
  accel.z,
  accel.timestamp
FROM trainer
JOIN customers
  ON trainer.serialNumber = customers.serialnumber
JOIN accel
  ON trainer.sensorReadingTime = accel.timestamp
'''
SQLQuery_node1751655454791 = sparkSqlQuery(glueContext, query = SqlQuery316, mapping = {"accel":acceletometer_trusted_node1751655402374, "trainer":step_trainer_trusted_node1751655401806, "customers":customer_curated_node1751657404929}, transformation_ctx = "SQLQuery_node1751655454791")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1751655454791, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751655386062", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1751655490575 = glueContext.getSink(path="s3://stedi-project-data-cei/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1751655490575")
machine_learning_curated_node1751655490575.setCatalogInfo(catalogDatabase="stedi_project_db",catalogTableName="machine_learning_curated_diagram")
machine_learning_curated_node1751655490575.setFormat("json")
machine_learning_curated_node1751655490575.writeFrame(SQLQuery_node1751655454791)
job.commit()