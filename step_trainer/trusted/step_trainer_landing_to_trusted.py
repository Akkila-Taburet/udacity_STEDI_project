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

# Script generated for node get customer_curated
getcustomer_curated_node1753791072806 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="customer_curated", transformation_ctx="getcustomer_curated_node1753791072806")

# Script generated for node Get step_trainer_landing
Getstep_trainer_landing_node1753791029391 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="step_trainer_landing", transformation_ctx="Getstep_trainer_landing_node1753791029391")

# Script generated for node SQL Query
SqlQuery0 = '''
select step.sensorreadingtime, step.serialnumber, step.distancefromobject from  cust left join step on step.serialnumber = cust.serialnumber
'''
SQLQuery_node1753791150585 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":Getstep_trainer_landing_node1753791029391, "cust":getcustomer_curated_node1753791072806}, transformation_ctx = "SQLQuery_node1753791150585")

# Script generated for node create step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1753791150585, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753790497373", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
createstep_trainer_trusted_node1753791280062 = glueContext.getSink(path="s3://udacity-stedi-akkila/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="createstep_trainer_trusted_node1753791280062")
createstep_trainer_trusted_node1753791280062.setCatalogInfo(catalogDatabase="akkila-stedi",catalogTableName="step_trainer_trusted")
createstep_trainer_trusted_node1753791280062.setFormat("json")
createstep_trainer_trusted_node1753791280062.writeFrame(SQLQuery_node1753791150585)
job.commit()
