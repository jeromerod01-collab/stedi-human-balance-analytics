"""machine_learning_curated.py
Trusted -> Curated: each Step Trainer reading joined with the accelerometer reading
taken at the same moment (sensorReadingTime = timestamp). Both inputs are already limited
to consenting customers. Email/user is not carried into the output, so the ML table
holds no direct PII. Creates stedi.machine_learning_curated.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """Glue Studio "Transform - SQL Query" node."""
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "s3://stedi-lakehouse-84164"
DATABASE = "stedi"

# Source: Data Catalog - step_trainer_trusted
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node",
)

# Source: Data Catalog - accelerometer_trusted
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# Transform - SQL Query (two parents): inner join on reading time = timestamp
JoinOnTimestamp_query = """
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
  ON s.sensorreadingtime = a.timestamp
"""
JoinOnTimestamp_node = sparkSqlQuery(
    glueContext,
    query=JoinOnTimestamp_query,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node,
        "accelerometer_trusted": AccelerometerTrusted_node,
    },
    transformation_ctx="JoinOnTimestamp_node",
)

# Target: Amazon S3 + Data Catalog (create table / update schema on subsequent runs)
MachineLearningCurated_node = glueContext.getSink(
    path=f"{BUCKET}/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node",
)
MachineLearningCurated_node.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName="machine_learning_curated")
MachineLearningCurated_node.setFormat("json")
MachineLearningCurated_node.writeFrame(JoinOnTimestamp_node)

job.commit()
