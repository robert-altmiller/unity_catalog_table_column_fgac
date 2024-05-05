import sys
sys.path.append('../db_setup')

from db_setup.main import *

# The dictionary `table_updates` is intended to hold information about updates
# to be applied to the table, specifying table name, row id, column to be updated, and the new value.
# table_updates = {
#     "0": ["my_managed_table", 1, "name", "Bill"],
#     "1": ["my_managed_table", 3, "city", "Los Angeles"],
#     "2": ["my_managed_table", 2, "age", 35],
# }

def execute_uc_table_updates(catalog_name, schema_name, update_table_function, check_perms_function, table_updates):
    """
    Executes updates on a managed table, verifying column permissions before applying each update.
    This function utilizes Spark SQL to check permissions and execute update commands dynamically.

    Parameters:
    catalog_name (str): Name of the catalog where the schema is located.
    schema_name (str): Name of the schema containing the table to update.
    update_table_function (str): Name of the SQL function that constructs the update SQL command.
    check_perms_function (str): Name of the SQL function that checks permission for a given column.
    table_updates (dict): A dictionary containing update instructions with keys as update identifiers and values as update parameters.
    """
    spark = SparkSession.builder.appName("execute_uc_table_updates").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    for key, val in table_updates.items():
        # Check column permissions using the permissions function defined in the database schema.
        query_perms = spark.sql(f"SELECT {check_perms_function} ('{val[0]}', '{val[2]}') AS query").collect()[0]['query']
        result_perms = spark.sql(query_perms).collect()
        group_name = result_perms[0]["group_name"]
        group_perms = result_perms[0]["group_perms"]

        # Check to see if the column can be modified and if the user has permissions to update the column.
        query_updates = spark.sql(f"SELECT {update_table_function} ('{val[0]}', '{val[1]}', '{val[2]}', '{val[3]}') AS query").collect()[0]['query']
        user_is_allowed_to_update = spark.sql(f"SELECT is_account_group_member('{group_name}') AS is_member").collect()[0]["is_member"]
        if group_perms == "MODIFY" and user_is_allowed_to_update:
            result_updates = spark.sql(query_updates).collect()[0]
            if result_updates["num_affected_rows"] > 0:
                print(f"UPDATE SUCCEEDED:\n'query: {query_updates}';\ncolumn_modify_perms: {group_perms};\nuser_allowed_to_update: {user_is_allowed_to_update}\n")
        else:
            print(f"UPDATE FAILED:\nquery: {query_updates};\ncolumn_modify_perms: {group_perms};\nuser_allowed_to_update: {user_is_allowed_to_update}\n")
    
    # Display the updates and verify the changes in the table.
    show_table(catalog_name, schema_name, val[0])
