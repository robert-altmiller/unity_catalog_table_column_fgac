from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_catalog_schema(catalog_name, schema_name):
    spark = SparkSession.builder.appName("create_catalog_schema").getOrCreate()

    SQL = f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`"
    print(SQL)
    spark.sql(SQL)
    SQL = f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"
    print(f"{SQL}\n")
    spark.sql(SQL)
    use_catalog_schema(catalog_name, schema_name)

def use_catalog_schema(catalog_name, schema_name):
    spark = SparkSession.builder.appName("use_catalog_schema").getOrCreate()

    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")


def show_table(catalog_name, schema_name, table_name):
    spark = SparkSession.builder.appName("show_table").getOrCreate()
    use_catalog_schema(catalog_name, schema_name)
    
    df = spark.sql(f"SELECT * FROM {table_name}")
    first_col = df.columns[0]
    df.sort(col(first_col).asc()).show()


def create_sample_table(catalog_name, schema_name, table_name = "my_managed_table"):
    spark = SparkSession.builder.appName("create_sample_table").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    SQL = f"DROP TABLE IF EXISTS {table_name}"
    print(SQL)
    spark.sql(SQL)
    
    SQL = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT,
        name STRING,
        age INT,
        city STRING
    )
    USING DELTA
    """
    print(SQL)
    spark.sql(SQL)


def insert_sample_table_data(catalog_name, schema_name, table_name = "my_managed_table"):
    spark = SparkSession.builder.appName("insert_sample_table_data").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)

    SQL = f"""
    INSERT INTO {table_name} (id, name, age, city)
    VALUES (1, 'John', 25, 'New York'),
           (2, 'Alice', 30, 'San Francisco'),
           (3, 'Bob', 35, 'Seattle')
    """
    print(SQL)
    spark.sql(SQL)
    show_table(catalog_name, schema_name, table_name)
    

def create_sample_table_perms(catalog_name, schema_name, table_name = "my_managed_table_perms"):
    spark = SparkSession.builder.appName("create_sample_table_permissions").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    SQL = f"DROP TABLE IF EXISTS {table_name}"
    print(SQL)
    spark.sql(SQL)
    SQL = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT,
        table_name STRING,
        column_name STRING,
        group_name STRING,
        group_perms STRING
    )
    USING DELTA
    """
    print(SQL)
    spark.sql(SQL)


def insert_sample_table_perms_data(catalog_name, schema_name, table_name = "my_managed_table_perms", group_name = "dev-contributors"):
    spark = SparkSession.builder.appName("insert_sample_table_perms_data").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    SQL = f"""
    INSERT INTO {table_name} (id, table_name, column_name, group_name, group_perms)
    VALUES (1, 'my_managed_table', 'name', '{group_name}', 'MODIFY'),
           (2, 'my_managed_table', 'age', '{group_name}', 'MODIFY'),
           (3, 'my_managed_table', 'city', '{group_name}', 'SELECT')
    """
    print(SQL)
    spark.sql(SQL)
    show_table(catalog_name, schema_name, table_name)


def create_sql_update_table_row_column_function(catalog_name, schema_name, function_name = "update_table_subset_columns"):
    spark = SparkSession.builder.appName("create_sql_update_table_column_function").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)

    drop_sql_function(catalog_name, schema_name, function_name)

    SQL = f"""
    CREATE OR REPLACE FUNCTION {function_name} (table_name STRING, row_id INT, col_to_update STRING, col_to_update_val STRING)
    RETURNS STRING
    RETURN 'UPDATE ' || table_name || ' SET ' || col_to_update || ' = ' || "'" || col_to_update_val || "'" || ' WHERE id = ' || row_id
    """
    print(SQL)
    spark.sql(SQL)


def create_sql_check_perms_function(catalog_name, schema_name, function_name = "update_table_subset_columns_perms"):
    spark = SparkSession.builder.appName("create_sql_check_perms_function").getOrCreate()
    create_catalog_schema(catalog_name, schema_name)
    
    drop_sql_function(catalog_name, schema_name, function_name)
    
    SQL = f"""
    CREATE OR REPLACE FUNCTION {function_name} (table_name STRING, column_name STRING)
    RETURNS STRING
    RETURN 'SELECT group_name, group_perms FROM my_managed_table_perms WHERE table_name = ' || "'" || table_name || "'" || ' AND column_name = ' || "'" || column_name || "'"
    """
    print(SQL)
    spark.sql(SQL)


def drop_sql_function(catalog_name, schema_name, function_name):
    spark = SparkSession.builder.appName("drop_sql_function").getOrCreate()
    use_catalog_schema(catalog_name, schema_name)
    
    SQL = f"DROP FUNCTION IF EXISTS `{catalog_name}`.`{schema_name}`.`{function_name}`"
    print(SQL)
    spark.sql(SQL)
    

def drop_table(catalog_name, schema_name, table_name):
    spark = SparkSession.builder.appName("drop_table").getOrCreate()
    use_catalog_schema(catalog_name, schema_name)
    
    SQL = f"DROP TABLE IF EXISTS `{catalog_name}`.`{schema_name}`.`{table_name}`"
    print(SQL)
    spark.sql(SQL)


def drop_catalog(catalog_name):
    spark = SparkSession.builder.appName("drop_catalog").getOrCreate()
    
    SQL = f"DROP CATALOG IF EXISTS `{catalog_name}`"
    print(SQL)
    spark.sql(SQL)
    

def drop_schema(catalog_name, schema_name):
    spark = SparkSession.builder.appName("drop_schema").getOrCreate()
    use_catalog_schema(catalog_name, schema_name)
    
    SQL = f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}`"
    print(SQL)
    spark.sql(SQL)