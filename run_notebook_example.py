# Databricks notebook source
# DBTITLE 1,Library Imports
from db_setup.main import create_sample_table
from db_setup.main import insert_sample_table_data 
from db_setup.main import create_sample_table_perms
from db_setup.main import insert_sample_table_perms_data
from db_setup.main import create_sql_update_table_row_column_function
from db_setup.main import create_sql_check_perms_function
from db_setup.main import create_sql_check_perms_function
from db_setup.main import drop_sql_function, drop_table, drop_catalog, drop_schema
from update_data.main import execute_uc_table_updates

# COMMAND ----------

# DBTITLE 1,Local Parameters
catalog_name = "my_catalog"
schema_name = "my_schema"
table_name = "my_managed_table"
table_name_permissions = "my_managed_table_perms"
table_name_permissions_group = "dev-contributors"
update_table_function = "update_table_function"
check_perms_function = "update_table_perms_function" 

# COMMAND ----------

# DBTITLE 1,Create Sample Table
create_sample_table(catalog_name, schema_name, table_name = table_name)

# COMMAND ----------

# DBTITLE 1,Insert Sample Table Data
insert_sample_table_data(catalog_name, schema_name, table_name = table_name)

# COMMAND ----------

# DBTITLE 1,Crate Sample Table Permissions
create_sample_table_perms(catalog_name, schema_name, table_name = table_name_permissions)

# COMMAND ----------

# DBTITLE 1,Insert Sample Table Permissions Data
insert_sample_table_perms_data(catalog_name, schema_name, table_name = table_name_permissions, group_name = table_name_permissions_group)

# COMMAND ----------

# DBTITLE 1,Create SQL Update Table Row Column Function
create_sql_update_table_row_column_function(catalog_name, schema_name, function_name = update_table_function)

# COMMAND ----------

# DBTITLE 1,Create SQL Check Permissions Function
create_sql_check_perms_function(catalog_name, schema_name, function_name = check_perms_function)

# COMMAND ----------

# DBTITLE 1,Execute Unity Catalog Managed Table Updates With Column Fine Grained Access Control
table_updates = {
    "0": ["my_managed_table", 1, "name", "Bill"],
    "1": ["my_managed_table", 3, "city", "Los Angeles"],
    "2": ["my_managed_table", 2, "age", 35],
}

execute_uc_table_updates(catalog_name, schema_name, update_table_function, check_perms_function, table_updates)

# COMMAND ----------

# DBTITLE 1,Cleanup Environment
drop_table(catalog_name, schema_name, table_name)
drop_table(catalog_name, schema_name, table_name_permissions)
drop_sql_function(catalog_name, schema_name, update_table_function) 
drop_sql_function(catalog_name, schema_name, check_perms_function) 
drop_schema(catalog_name, schema_name)
drop_schema(catalog_name, "default")
drop_catalog(catalog_name)
