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

session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()


df_customer_info = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df_customer_filter = df_customer_info.filter(col("C_MKTSEGMENT") == 'HOUSEHOLD')
df_customer_select = df_customer_info.select(col("C_NAME"), col("C_ADDRESS"))
df_customer_select.show()
df_customer_select.count()


df_customer_select.describe().sort("SUMMARY").show()






