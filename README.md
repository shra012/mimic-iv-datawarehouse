# Data Warehouse for MIMIC-IV medical database cost analysis
This project has the necessary schema design for a data warehouse using the MIMIC-IV medical data for cost analysis.

# Run this project using docker in local with the following command
```bash
docker run -it --rm  -v ~/.aws:/home/hadoop/.aws \
  -v /Users/hiruzen/Programming/Projects/mimic-iv-datawarehouse:/home/hadoop/workspace/ \
  -e AWS_PROFILE=default \
  --name cost public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/glue/simple_cost_analysis_elt.py --JOB_NAME MIMIC_COST_ANALYSYS
```
```bash
docker run -it --rm  -v ~/.aws:/home/hadoop/.aws \
 -v /Users/hiruzen/Programming/Projects/mimic-iv-datawarehouse:/home/hadoop/workspace/ \
 -e AWS_PROFILE=default \
 --name cost public.ecr.aws/glue/aws-glue-libs:5 \
 spark-submit /home/hadoop/workspace/glue/redshift_ingestor.py --JOB_NAME MIMIC_INGESTION
```
Remember to configure your AWS credentials in `~/.aws` directory. You can use the following command to configure your AWS credentials:
```bash
aws configure
```
Get your AWS credentials from the AWS console. You should create a new IAM user with the following permissions
- AmazonS3FullAccess: To allow Glue to read source data from S3 and write the output files.
- AWSGlueServiceRole: This role is assumed by the Glue job to access required AWS services. It should include permissions to run Glue jobs.
- AWSGlueConsoleFullAccess: For full access to the Glue console (if you’re managing jobs interactively).
- AmazonRedshiftFullAccess (or RedshiftFullAccess): To permit operations on Redshift, such as creating clusters, loading data via the COPY command, and running queries.

When your Glue job is submitted, it will assume the specified Glue service role which should have these policies attached. This setup lets the job read from S3 and push data to Redshift without additional permissions.

However, keep these points in mind:
- Ensure that the Glue service role has a trust relationship that allows Glue to assume the role.
- Validate that your S3 bucket policies and Redshift cluster security groups (or VPC settings) allow access from your Glue job.