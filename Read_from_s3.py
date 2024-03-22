from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"lr58752.ap-south-1",
"user":"Lowekumar",
"password": "Rudra@98351",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DB1",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

# Mention command to read data from '@my_s3_stage_json'
employee_s3_json = session.read.json('@my_s3_stage_jsondata')
# employee_s3_json.show()
# employee_s3_json.cache_result()

# employee_s3_json = employee_s3_json.select(col("$1").as_("new_col")).show()

employee_s3_json = employee_s3_json.select_expr("$1:author","$1:id","$1:cat")
employee_s3_json.show()
# employee_s3_json.cache_result()

# employee_s3_json.schema