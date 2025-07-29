import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node get_customer_trusted
get_customer_trusted_node1753778995396 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="customer_trusted", transformation_ctx="get_customer_trusted_node1753778995396")

# Script generated for node get accelerometer_trusted
getaccelerometer_trusted_node1753779067509 = glueContext.create_dynamic_frame.from_catalog(database="akkila-stedi", table_name="accelerometer_trusted", transformation_ctx="getaccelerometer_trusted_node1753779067509")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
DISTINCT
serialnumber,
registrationdate,
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate from cust left join acc on cust.email = acc.user
where acc.timestamp > cust.sharewithresearchasofdate
'''
SQLQuery_node1753779168375 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cust":get_customer_trusted_node1753778995396, "acc":getaccelerometer_trusted_node1753779067509}, transformation_ctx = "SQLQuery_node1753779168375")

# Script generated for node create customer_curated
createcustomer_curated_node1753779334441 = glueContext.getSink(path="s3://udacity-stedi-akkila/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="createcustomer_curated_node1753779334441")
createcustomer_curated_node1753779334441.setCatalogInfo(catalogDatabase="akkila-stedi",catalogTableName="customer_curated")
createcustomer_curated_node1753779334441.setFormat("json")
createcustomer_curated_node1753779334441.writeFrame(SQLQuery_node1753779168375)
job.commit()
