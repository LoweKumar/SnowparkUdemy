
import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# connection_parameters = {"account":"********",
# "user":"*****",
# "password": "*******",
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

print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())

#2 Display EMPFNAME and “TOTAL SALARY” for each employee
# SELECT EMPFNAME, SUM(COMMISSION+SALARY) AS "TOTAL SALARY" FROM EMPLY GROUP BY EMPFNAME;

#Read EMPLY table from snowflake and group data by column EMPFNAME and create dataframe by name, total_sal_group_by
emp_stg_tbl = session.table("DB1.PUBLIC.EMPLY")
total_sal_group_by = emp_stg_tbl.group_by("EMPFNAME")

# Aggregate columns, SALARY and COMMISSION

total_salary = total_sal_group_by.agg((col("SALARY")+col("COMMISSION"), "sum"))
# total_salary = total_sal_group_by.agg([col("COMMISSION")+col("SALARY"),"sum"])

from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_
total_salary = emp_stg_tbl.group_by("EMPFNAME").agg(sum_(col("COMMISSION")+col("SALARY")).alias("TOTAL_SALARY"))
result = total_salary.collect()
print(result)
