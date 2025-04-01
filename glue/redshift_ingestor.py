import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_base = "s3://mimic-iv-datas/glue"
DIM_PATIENT_PATH = f"{s3_base}/parquet/dim_patient/"
DIM_DATE_PATH = f"{s3_base}/parquet/dim_date/"
DIM_ICU_UNIT_PATH = f"{s3_base}/parquet/dim_icu_stay/"
DIM_ADMISSION_PATH = f"{s3_base}/parquet/dim_admission/"
FACT_ICUSTAY_PATH = f"{s3_base}/parquet/fact_icustay/"

dim_patient_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [DIM_PATIENT_PATH]},
    transformation_ctx="dim_patient_dyf"
)

dim_date_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [DIM_DATE_PATH]},
    transformation_ctx="dim_date_dyf"
)

dim_icu_unit_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [DIM_ICU_UNIT_PATH]},
    transformation_ctx="dim_icu_unit_dyf"
)

dim_admission_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [DIM_ADMISSION_PATH]},
    transformation_ctx="dim_admission_dyf"
)

fact_icustay_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [FACT_ICUSTAY_PATH]},
    transformation_ctx="fact_icustay_dyf"
)

redshift_connection_name = "redshift-serverless-connection"
redshift_database = "dev"
redshift_temp_dir = f"{s3_base}/redshift/"

preaction_dim_patient = "TRUNCATE TABLE public.dim_patient"
preaction_dim_date = "TRUNCATE TABLE public.dim_date"
preaction_dim_icu_unit = "TRUNCATE TABLE public.dim_icu_unit"
preaction_dim_admission = "TRUNCATE TABLE public.dim_admission"
preaction_fact_icustay = "TRUNCATE TABLE public.fact_icustay"

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dim_patient_dyf,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": "public.dim_patient",
        "database": redshift_database,
        "preactions": preaction_dim_patient
    },
    redshift_tmp_dir=redshift_temp_dir,
    transformation_ctx="dim_patient_to_rs"
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dim_date_dyf,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": "public.dim_date",
        "database": redshift_database,
        "preactions": preaction_dim_date
    },
    redshift_tmp_dir=redshift_temp_dir,
    transformation_ctx="dim_date_to_rs"
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dim_icu_unit_dyf,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": "public.dim_icu_unit",
        "database": redshift_database,
        "preactions": preaction_dim_icu_unit
    },
    redshift_tmp_dir=redshift_temp_dir,
    transformation_ctx="dim_icu_unit_to_rs"
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dim_admission_dyf,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": "public.dim_admission",
        "database": redshift_database,
        "preactions": preaction_dim_admission
    },
    redshift_tmp_dir=redshift_temp_dir,
    transformation_ctx="dim_admission_to_rs"
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=fact_icustay_dyf,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": "public.fact_icustay",
        "database": redshift_database,
        "preactions": preaction_fact_icustay
    },
    redshift_tmp_dir=redshift_temp_dir,
    transformation_ctx="fact_icustay_to_rs"
)

job.commit()