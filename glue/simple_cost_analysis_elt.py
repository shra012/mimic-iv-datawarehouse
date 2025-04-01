import sys
import re
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, to_date, date_format, expr


def extract_icu_type(icu_name):
    match = re.search(r'\((.*?)\)', icu_name)
    if match:
        content = match.group(1)
        if content == "":
            return None
        elif "/" in content:
            return content.split("/")[0]
        else:
            return content
    return None


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_base = "s3://mimic-iv-datas"
s3_hosp = f"{s3_base}/hosp"
s3_icu = f"{s3_base}/icu"
admissions_df = spark.read.format("csv").option("header", "true").load(f"{s3_hosp}/admissions.csv")
patients_df = spark.read.format("csv").option("header", "true").load(f"{s3_hosp}/patients.csv")
drgcodes_df = spark.read.format("csv").option("header", "true").load(f"{s3_hosp}/drgcodes.csv")
icustays_df = spark.read.format("csv").option("header", "true").load(f"{s3_icu}/icustays.csv")

extract_icu_type_udf = udf(extract_icu_type, StringType())
patients_admissions_joined = patients_df.alias("pat").join(admissions_df.alias("adm"), ["subject_id"], "left")
dim_patient = patients_admissions_joined.select(
    F.col("pat.subject_id").alias("patient_id"),
    F.col("pat.gender"),
    F.col("adm.race"),
    F.col("pat.anchor_year").alias("birth_year"),
    F.col("adm.marital_status"),
    F.col("pat.anchor_age").alias("age"),
).distinct()

admissions_joined = admissions_df.alias("adm").join(drgcodes_df.alias("drg"), ["hadm_id"], "left")
dim_admission = admissions_joined.select(
    F.col("adm.hadm_id").alias("icu_unit_name"),
    F.col("adm.admission_type"),
    F.col("adm.admission_location").alias("admission_source"),
    F.col("adm.discharge_location").alias("discharge_disposition"),
    F.col("adm.insurance").alias("insurance_category"),
    F.col("drg.drg_code"),
    F.col("drg.description").alias("drg_description"),
    F.col("drg.drg_type").alias("drg_category"),
).distinct()

dim_icu_unit = icustays_df.select(
    F.col("stay_id").alias("unit_id"),
    extract_icu_type_udf(F.col("first_careunit")).alias("unit_type"),
    F.col("first_careunit").alias("unit_name"),
).distinct()

admission_icustays = admissions_df.alias("adm").join(icustays_df.alias("icu"), "hadm_id", "inner")
fact_icustay = admission_icustays.select(
    F.col("icu.subject_id").alias("patient_id"),
    F.col("icu.hadm_id").alias("admission_id"),
    F.col("icu.stay_id").alias("stay_id"),
    F.col("icu.intime").alias("icu_admit_date"),
    F.col("icu.outtime").alias("icu_discharge_date"),
    F.col("icu.los").alias("total_icu_days")
)

dates_admit_df = fact_icustay.select(to_date(F.col("icu_admit_date")).alias("calendar_date")).distinct()
dates_discharge_df = fact_icustay.select(to_date(F.col("icu_discharge_date")).alias("calendar_date")).distinct()
all_dates_df = dates_admit_df.union(dates_discharge_df).distinct()
dim_date_df = (all_dates_df.withColumn("date_id", date_format(F.col("calendar_date"), "yyyyMMdd"))
               .withColumn("year", date_format(F.col("calendar_date"), "yyyy"))
               .withColumn("quarter", expr("quarter(calendar_date)"))
               .withColumn("month", date_format(F.col("calendar_date"), "MM"))
               .withColumn("day_of_week", date_format(F.col("calendar_date"), "EEEE"))
               .filter(F.col("date_id").isNotNull()))

s3_out = "s3://mimic-iv-datas/glue/parquet"
dim_patient.write.mode("overwrite").parquet(f"{s3_out}/dim_patient/")
dim_admission.write.mode("overwrite").parquet(f"{s3_out}/dim_admission/")
dim_icu_unit.write.mode("overwrite").parquet(f"{s3_out}/dim_icu_stay/")
dim_date_df.write.mode("overwrite").parquet(f"{s3_out}/dim_date/")
fact_icustay.write.mode("overwrite").parquet(f"{s3_out}/fact_icustay/")

job.commit()
