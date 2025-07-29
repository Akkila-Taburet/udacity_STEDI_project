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

# Script generated for node get accelerometer_landing
getaccelerometer_landing_node1753776833038 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="accelerometer_landing", transformation_ctx="getaccelerometer_landing_node1753776833038")

# Script generated for node get customer_landing
getcustomer_landing_node1753731169333 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="customer_trusted", transformation_ctx="getcustomer_landing_node1753731169333")

# Script generated for node SQL Query
SqlQuery0 = '''
select user,timestamp,x,z,y from acc right join cust on acc.user = cust.email where acc.timestamp > cust.sharewithresearchasofdate
'''
SQLQuery_node1753776867367 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cust":getcustomer_landing_node1753731169333, "acc":getaccelerometer_landing_node1753776833038}, transformation_ctx = "SQLQuery_node1753776867367")

# Script generated for node create accelermoter_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1753776867367, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753775926207", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
createaccelermoter_trusted_node1753777028620 = glueContext.getSink(path="s3://udacity-stedi-akkila/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="createaccelermoter_trusted_node1753777028620")
createaccelermoter_trusted_node1753777028620.setCatalogInfo(catalogDatabase="akkila-stedi",catalogTableName="accelerometer_trusted")
createaccelermoter_trusted_node1753777028620.setFormat("json")
createaccelermoter_trusted_node1753777028620.writeFrame(SQLQuery_node1753776867367)
job.commit()
