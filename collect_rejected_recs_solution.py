# Create table in snowflake

# create or replace TABLE EMPLOYEE (
# 	FIRST_NAME VARCHAR(16777216),
# 	LAST_NAME VARCHAR(16777216),
# 	EMAIL VARCHAR(16777216),
# 	ADDRESS VARCHAR(16777216),
# 	CITY VARCHAR(16777216),
# 	DOJ DATE
# );

from snowflake.snowpark import Session
from snowflake.snowpark import QueryRecord
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"eg24753.ap-south-1",
"user":"Lowekumar28",
"password": "Rudra@983517",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DB1",
"schema":"PUBLIC"
}

# # To test connection
# test_session = Session.builder.configs(connection_parameters).create()

# print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

# session = Session.builder.configs(connection_parameters).create()
# Connection test end


session = Session.builder.configs(connection_parameters).create()

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@external_stage/')

# Modify below command to so that you can get query id from sesssion.
with session.query_history() as query_history:
    copied_into_result = employee_s3.copy_into_table("employee", target_columns=['FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','CITY','DOJ'], force=True,on_error="CONTINUE")

query = query_history.queries

# Mention command to collect query id of copy command executed.
for id in query:
    if "COPY" in id.sql_text:
        qid = id.query_id

# Mention command to collect rejected records from the query id.
rejects = session.sql("select *  from table(validate(employee , job_id =>"+ "'"+ qid +"'))")
rejects.show()