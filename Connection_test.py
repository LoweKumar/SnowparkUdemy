import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {"account":"lr58752.ap-south-1",
"user":"Lowekumar",
"password": "***",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"SNOWFLAKE_SAMPLE_DATA",
"schema":"TPCH_SF1"
}

test_session = Session.builder.configs(connection_parameters).create()

print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

session = Session.builder.configs(connection_parameters).create()
