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

# Script generated for node get customer_landing
getcustomer_landing_node1753727695806 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="customer_landing", transformation_ctx="getcustomer_landing_node1753727695806")

# Script generated for node remove shareWithResearchAsOfDate
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
removeshareWithResearchAsOfDate_node1753727795256 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":getcustomer_landing_node1753727695806}, transformation_ctx = "removeshareWithResearchAsOfDate_node1753727795256")

# Script generated for node Create Customer_trusted
EvaluateDataQuality().process_rows(frame=removeshareWithResearchAsOfDate_node1753727795256, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753728110113", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CreateCustomer_trusted_node1753728347399 = glueContext.getSink(path="s3://udacity-stedi-akkila/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CreateCustomer_trusted_node1753728347399")
CreateCustomer_trusted_node1753728347399.setCatalogInfo(catalogDatabase="akkila-stedi",catalogTableName="customer_trusted")
CreateCustomer_trusted_node1753728347399.setFormat("json")
CreateCustomer_trusted_node1753728347399.writeFrame(removeshareWithResearchAsOfDate_node1753727795256)
job.commit()
