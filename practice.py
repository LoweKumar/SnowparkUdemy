# pip install snowflake-connector-python

import snowflake.connector
# https://jj54445.ap-south-1.aws.snowflakecomputing.com
# https://lr58752.ap-south-1.aws.snowflakecomputing.com
ctx = snowflake.connector.connect(
    user='Lowekumar',
    password = 'Rudra@98351',
    account = 'lr58752',
    region ='ap-south-1.aws',
    Role = 'ACCOUNTADMIN',
    Warehouse = 'COMPUTE_WH'
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()