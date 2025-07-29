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

# Script generated for node get accelerometer_trusted
getaccelerometer_trusted_node1753794690254 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="accelerometer_trusted", transformation_ctx="getaccelerometer_trusted_node1753794690254")

# Script generated for node get step_trainer_trusted
getstep_trainer_trusted_node1753794649571 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="step_trainer_trusted", transformation_ctx="getstep_trainer_trusted_node1753794649571")

# Script generated for node SQL Query
SqlQuery0 = '''
select sensorreadingtime,serialnumber,distancefromobject,timestamp,x,y,z  from  step inner join  acc on step.sensorreadingtime = acc.timestamp

'''
SQLQuery_node1753794725321 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":getstep_trainer_trusted_node1753794649571, "acc":getaccelerometer_trusted_node1753794690254}, transformation_ctx = "SQLQuery_node1753794725321")

# Script generated for node create Machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1753794725321, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753790497373", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
createMachine_learning_curated_node1753794933431 = glueContext.getSink(path="s3://udacity-stedi-akkila/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="createMachine_learning_curated_node1753794933431")
createMachine_learning_curated_node1753794933431.setCatalogInfo(catalogDatabase="akkila-stedi",catalogTableName="machine_learning_curated")
createMachine_learning_curated_node1753794933431.setFormat("json")
createMachine_learning_curated_node1753794933431.writeFrame(SQLQuery_node1753794725321)
job.commit()
