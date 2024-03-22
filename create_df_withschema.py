from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

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
session.sql("use warehouse compute_wh").collect()

schema = StructType([StructField("one", IntegerType()),
StructField("two",  IntegerType()),
StructField("three",  IntegerType()),
StructField("four",  DateType())])

test = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26'],[1, 2, 3, '2022-01-26']], schema=schema)
test.show()