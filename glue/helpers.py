import re
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.types import StringType

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
#
#
# def build_drug_gsn_mapping_bc(prescriptions_df, sc):
#     raw_gsn_df = prescriptions_df.select(
#         F.col("drug").alias("drug_name"),
#         F.col("gsn").alias("raw_gsn")
#     ).filter((F.col("raw_gsn").isNotNull()) & (F.col("raw_gsn") != ""))
#     tokenized_df = raw_gsn_df.withColumn("gsn_tokens", F.split(F.regexp_replace("raw_gsn", ",", " "), "\s+"))
#     exploded_df = tokenized_df.select("drug_name", F.explode("gsn_tokens").alias("gsn_token"))
#     exploded_df = exploded_df.withColumn("gsn_token", F.trim(F.col("gsn_token")))
#     frequency_df = exploded_df.groupBy("drug_name", "gsn_token").agg(F.count("*").alias("token_count"))
#     window_spec = Window.partitionBy("drug_name").orderBy(F.desc("token_count"))
#     ranked_df = frequency_df.withColumn("rank", F.row_number().over(window_spec))
#     most_freq_df = ranked_df.filter(F.col("rank") == 1).select("drug_name", F.col("gsn_token").alias("most_freq_gsn"))
#     drug_gsn_mapping = {row["drug_name"]: row["most_freq_gsn"] for row in most_freq_df.collect()}
#     return sc.broadcast(drug_gsn_mapping)
#
#
# def resolve_gsn(drug_name, gsn_field, drug_gsn_mapping_bc):
#     if gsn_field and gsn_field.strip() != "":
#         tokens = [token.strip() for token in gsn_field.replace(",", " ").split() if token.strip() != ""]
#         if len(tokens) == 1:
#             return tokens[0]
#         else:
#             mapping = drug_gsn_mapping_bc.value
#             if drug_name in mapping:
#                 return mapping[drug_name]
#             else:
#                 return tokens[0] if tokens[0] else "00000"
#     return "00000"