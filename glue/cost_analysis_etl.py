# import sys
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import udf, lit
# from helpers import  build_drug_gsn_mapping_bc, resolve_gsn, extract_icu_type
#
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
#
# # ------------------------------------------------------------------------------
# # 1. Data Extraction
# # ------------------------------------------------------------------------------
#
# admissions_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/admissions.csv")
# patients_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/patients.csv")
# drgcodes_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/drgcodes.csv")
# diagnoses_icd_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/diagnoses_icd.csv")
# d_diagnoses_icd_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/d_icd_diagnoses.csv")
# procedures_icd_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/procedures_icd.csv")
# d_procedures_icd_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/procedures_icd.csv")
# icustays_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/icu/icustays.csv")
# labevents_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/labevents.csv")
# prescriptions_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/prescriptions.csv")
# d_labitems_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/d_labitems.csv")
# d_hcpcs_df = spark.read.format("csv").option("header", "true").load("s3://mimic-iv-datas/hosp/d_hcpcs.csv")
#
#
# drug_gsn_mapping_bc = build_drug_gsn_mapping_bc(prescriptions_df, sc)
# resolve_gsn_udf = udf(lambda drug, gsn: resolve_gsn(drug, gsn, drug_gsn_mapping_bc), StringType())
# extract_icu_type_udf = udf(extract_icu_type, StringType())
# patients_admissions_joined = patients_df.alias("pat").join(admissions_df.alias("adm"), ["subject_id"], "left")
# dim_patient = patients_df.select(
#     F.col("pat.subject_id").alias("patient_id"),
#     F.col("pat.gender"),
#     F.col("adm.race"),
#     F.col("pat.anchor_year").alias("birth_year"),
#     F.col("pat.marital_status"),
#     F.col("pat.anchor_age").alias("age"),
# ).distinct()
#
# admissions_joined = admissions_df.alias("adm").join(drgcodes_df.alias("drg"), ["hadm_id"], "left")
# dim_admission = admissions_joined.select(
#     F.col("adm.hadm_id").alias("icu_unit_name"),
#     F.col("adm.admission_type"),
#     F.col("adm.admission_location").alias("admission_source"),
#     F.col("adm.discharge_location").alias("discharge_disposition"),
#     F.col("adm.insurance").alias("insurance_category"),
#     F.col("drg.drg_code"),
#     F.col("drg.description").alias("drg_description"),
#     F.col("drg.drg_type").alias("drg_category"),
# ).distinct()
#
# dim_icu_unit = icustays_df.select(
#     F.col("stay_id").alias("unit_id"),
# extract_icu_type_udf(F.col("first_careunit")).alias("unit_type"),
#     F.col("first_careunit").alias("unit_name"),
# ).distinct()
#
# diagnoses_joined = diagnoses_icd_df.alias("diag").join(d_diagnoses_icd_df.alias("d_diag"), ["icd_code", "icd_version"], "left")
# dim_diagnosis = diagnoses_icd_df.select(
#     F.col("diag.icd_code"),
#     F.lit("d_diag.long_title").alias("description"),
#     F.lit("diag.icd_version").alias("icd_version")
# ).distinct()
#
# dim_medication = prescriptions_df.select(
#     F.col("gsn"),
#     F.col("drug").alias("drug_name"),
#     F.col("drug_class").alias("drug_class"),
#     F.col("form_rx").alias("form"),
#     F.col("dose_val_rx").alias("dosage"),
#     F.col("doses_per_24_hrs").alias("frequency"),
#     F.col("route").alias("route")
# ).distinct()
# dim_medication = dim_medication.withColumn("drug_id", resolve_gsn_udf(F.col("drug_name"), F.col("gsn")))
# dim_medication = dim_medication.drop("gsn")
#
# dim_labtest = d_labitems_df.select(
#     F.col("itemid").alias("lab_item_id"),
#     F.col("label").alias("test_name"),
#     F.col("category").alias("test_category"),
#     F.lit('fluid').alias("test_type")
# ).distinct()
#
# dim_hcpcs = d_hcpcs_df.select(
#     F.col("code").alias("hcpcs_code"),
#     F.col("long_description").alias("service_description"),
#     F.col("category").alias("category")
# ).distinct()
#
# admission_icustays = admissions_df.alias("adm").join(icustays_df.alias("icu"), "hadm_id", "left")
# fact_icustay = icustays_df.select(
#     F.col("icu.subject_id").alias("patient_id"),
#     F.col("icu.hadm_id").alias("admission_id"),
#     F.col("icu.stay_id").alias("stay_id"),
#     F.col("icu.intime").alias("icu_admit_date"),
#     F.col("icu.outtime").alias("icu_discharge_date"),
#     F.col("icu.los").alias("total_icu_days"),
#     F.col("icu.los").alias("total_icu_days")
# )
#
# lab_events = labevents_df.select(
#     F.col("subject_id").alias("patient_id"),
#     F.col("hadm_id").alias("admission_id"),
#     F.col("charttime").alias("event_date"),
#     F.col("itemid").alias("lab_item_id")
# ).withColumn("event_type", F.lit("LAB")) \
#  .withColumn("event_id", F.col("lab_item_id")) \
#  .withColumn("cost", lit(0)) \
#  .withColumn("location_id", lit(None).cast("string")) \
#  .withColumn("med_id", lit(None).cast("string")) \
#  .withColumn("hcpcs_cd", lit(None).cast("string")) \
#  .withColumn("procedure_code", lit(None).cast("string"))
#
# # Prescription cost placeholders
# med_events = prescriptions_df.select(
#     F.col("subject_id").alias("patient_id"),
#     F.col("hadm_id").alias("admission_id"),
#     F.col("starttime").alias("event_date"),
#     F.col("drug").alias("drug_name"),
#     F.col("gsn").alias("raw_gsn")
# ).withColumn("event_type", F.lit("MED")) \
#  .withColumn("event_id", resolve_gsn_udf(F.col("drug_name"), F.col("raw_gsn"))) \
#  .withColumn("cost", lit(0)) \
#  .withColumn("lab_item_id", lit(None).cast("string")) \
#  .withColumn("hcpcs_cd", lit(None).cast("string")) \
#  .withColumn("procedure_code", lit(None).cast("string"))
#
# # Union lab and med events for a single Fact_CostEvent
# fact_costevent = lab_events.unionByName(med_events).withColumn("event_id", F.monotonically_increasing_id().cast("string"))
#
# # For demonstration, location_id, procedure_code, hcpcs_cd, etc. are omitted or left as placeholders.
#
# # ------------------------------------------------------------------------------
# # 5. Write Each Table to S3
# # ------------------------------------------------------------------------------
#
# dim_patient.write.mode("overwrite").parquet("s3://mimic-iv-datas/dim_patient/")
# dim_admission.write.mode("overwrite").parquet("s3://mimic-iv-datas/dim_admission/")
# dim_icu_unit.write.mode("overwrite").parquet("s3://mimic-iv-datas/dim_icu_stay/")
# dim_drg.write.mode("overwrite").parquet("s3://mimic-iv-datas/dim_drg/")
# dim_diagnosis.write.mode("overwrite").parquet("s3://mimic-iv-datas/dim_diagnosis/")
#
# fact_icustay.write.mode("overwrite").parquet("s3://mimic-iv-datas/fact_icustay/")
# fact_costevent.write.mode("overwrite").parquet("s3://mimic-iv-datas/fact_costevent/")
#
# job.commit()