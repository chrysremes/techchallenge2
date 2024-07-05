import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import gs_derived
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Read_S3_Raw
Read_S3_Raw_node1720057622748 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://s3-chrys-mlet-lab-tc2/raw/DataB3_last_day.parquet"]}, transformation_ctx="Read_S3_Raw_node1720057622748")

# Script generated for node Rename_Drop_and_Change_Schema
Rename_Drop_and_Change_Schema_node1720059032546 = ApplyMapping.apply(frame=Read_S3_Raw_node1720057622748, mappings=[("data", "string", "TS", "timestamp"), ("codigo", "string", "cod", "string"), ("acao", "string", "acao", "string"), ("qtde", "bigint", "qtde", "long"), ("setor_part", "double", "setor_part", "double"), ("setor_part_ac", "double", "setor_part_ac", "double")], transformation_ctx="Rename_Drop_and_Change_Schema_node1720059032546")

# Script generated for node Select_TS_Col
Select_TS_Col_node1720062140027 = SelectFields.apply(frame=Rename_Drop_and_Change_Schema_node1720059032546, paths=["TS", "cod", "acao", "qtde"], transformation_ctx="Select_TS_Col_node1720062140027")

# Script generated for node Is_TS_now_GT_TS
Is_TS_now_GT_TS_node1720062324400 = Select_TS_Col_node1720062140027.gs_derived(colName="new_TS", expr="CAST(CAST(TS as double) + qtde as timestamp)")

# Script generated for node Aggregate
Aggregate_node1720123687979 = sparkAggregate(glueContext, parentFrame = Select_TS_Col_node1720062140027, groups = [], aggs = [["qtde", "avg"], ["qtde", "max"], ["qtde", "min"]], transformation_ctx = "Aggregate_node1720123687979")

# Script generated for node Derived Column
DerivedColumn_node1720123994931 = Is_TS_now_GT_TS_node1720062324400.gs_derived(colName="date_diff", expr="DATEDIFF(TS, new_TS)")

# Script generated for node Rename Field
RenameField_node1720127840701 = RenameField.apply(frame=DerivedColumn_node1720123994931, old_name="TS", new_name="b3_timestamp", transformation_ctx="RenameField_node1720127840701")

# Script generated for node Rename Field
RenameField_node1720127861034 = RenameField.apply(frame=RenameField_node1720127840701, old_name="new_TS", new_name="fictious_timestamp", transformation_ctx="RenameField_node1720127861034")

# Script generated for node Amazon S3
AmazonS3_node1720127798909 = glueContext.getSink(path="s3://s3-chrys-mlet-lab-tc2/refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720127798909")
AmazonS3_node1720127798909.setCatalogInfo(catalogDatabase="default",catalogTableName="bovespa_statistics")
AmazonS3_node1720127798909.setFormat("glueparquet", compression="snappy")
AmazonS3_node1720127798909.writeFrame(Aggregate_node1720123687979)
# Script generated for node Amazon S3
AmazonS3_node1720127808449 = glueContext.getSink(path="s3://s3-chrys-mlet-lab-tc2/refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["date_diff", "cod"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720127808449")
AmazonS3_node1720127808449.setCatalogInfo(catalogDatabase="default",catalogTableName="bovespa_b3")
AmazonS3_node1720127808449.setFormat("glueparquet", compression="snappy")
AmazonS3_node1720127808449.writeFrame(RenameField_node1720127861034)
job.commit()