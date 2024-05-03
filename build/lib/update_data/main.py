import sys
sys.path.append('../db_setup')

from db_setup.main import *

# example updates for sample data table
# table_updates = {
#     "0": ["my_managed_table", 1, "name", "Bill"],
#     "1": ["my_managed_table", 3, "city", "Los Angeles"],
#     "2": ["my_managed_table", 2, "age", 35],
# }

def execute_uc_table_updates(catalog_name, schema_name, update_table_function, check_perms_function, table_updates):
    """execute uc managed table updates"""
    spark = SparkSession.builder.appName("create_sample_table").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    for key, val in table_updates.items():
        # check column permissions
        query_perms = spark.sql(f"SELECT {check_perms_function} ('{val[0]}', '{val[2]}') AS query").collect()[0]['query']
        result_perms = spark.sql(query_perms).collect()
        group_name = result_perms[0]["group_name"]
        group_perms = result_perms[0]["group_perms"]

        # check to see if the column can be modified and if the user has permissions to update the column
        query_updates = spark.sql(f"SELECT {update_table_function} ('{val[0]}', '{val[1]}', '{val[2]}', '{val[3]}') AS query").collect()[0]['query']
        user_is_allowed_to_update = spark.sql(f"SELECT is_account_group_member('{group_name}') AS is_member").collect()[0]["is_member"]
        if group_perms == "MODIFY" and user_is_allowed_to_update:
            result_updates = spark.sql(query_updates).collect()[0]
            if result_updates["num_affected_rows"] > 0: 
                print(f"UPDATE SUCCEEDED:\n'query: {query_updates}';\ncolumn_modify_perms: {group_perms};\nuser_allowed_to_update: {user_is_allowed_to_update}\n")
        else: print(f"UPDATE FAILED:\nquery: {query_updates};\ncolumn_modify_perms: {group_perms};\nuser_allowed_to_update: {user_is_allowed_to_update}\n")
    # display the updates and verify
    show_table(catalog_name, schema_name, val[0])