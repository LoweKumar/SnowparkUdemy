from pickle import TRUE
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

# Replace the below connection_parameters with your respective snowflake account,user name and password
# connection_parameters = {"account":"lr58752.ap-south-1",
# "user":"Lowekumar",
# "password": "***",
# "role":"ACCOUNTADMIN",
# "warehouse":"COMPUTE_WH",
# "database":"DB1",
# "schema":"PUBLIC"
# }

connection_parameters = {"account":"eg24753.ap-south-1",
"user":"Lowekumar28",
"password": "***",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DB1",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()

# Create a temp stage.
_ = session.sql("create or replace temp stage db1.public.mystage1").collect()

# Unload data from snowflake table employee to stage locaion @mystage/download/
emp_stg_tbl = session.table("DB1.PUBLIC.EMPLOYEE")
copy_result = emp_stg_tbl.write.copy_into_location('@mystage1/download/', file_format_type="csv", header=True, overwrite=True, single=True)
print(copy_result)


# Download files from internal stage to your local path
get_result1 = session.file.get("@mystage1/download/", "data/downloaded/emp/")
print(get_result1)
