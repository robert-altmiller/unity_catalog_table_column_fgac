# Unity Catalog Fine Grained Access Control for Managed Table Columns + Table Updates

When working with managed tables in Unity Catalog there are many different kinds of fine grained access controls (FGAC).  You can do column and row level masking for PII data.  You can grant individual groups access to an entire table.  What if I want to apply access controls for a subset of individual columns in UC managed table.  

For example if I have a table with '10 columns' I want to be able to grant "group1" access to make changes to "column1" in the table but I do not want them to be able to update any of the other 9 columns in the table.  This method is a combination of column level fine grained access controls + managed table updates at the same time.  I can also assign multiple groups to be able to make updates to other columns in the managed table and block them from being able to update column1.

This repo demonstration shows how to accomplish the example above using the code in this repo built as a python whl which the end user loads as Python library in on a Databricks cluster.

# How to get started right away

clone the repo: https://github.com/robert-altmiller/unity_catalog_table_column_fgac.git

The python whl can be located under 'dist' folder.  Load this whl on the Databricks cluster, and then copy the run_notebook_example.py into your Databricks workspace and run it.  When you run the notebook from top to bottom it will print out the sql which is getting executed to create the the sample data and permissions tables, table update and permission SQL functions, catalogs, and schemas.  At the end of the notebook there is an environment cleanup step which will remove all the resources that were created in your Databricks workspace.

# How to rebuild the Python whl

clone the repo: https://github.com/robert-altmiller/unity_catalog_table_column_fgac.git

To rebuild the python whl simply make updates to the Python code in the 'update_data' and 'db_setup' folders.  The 'main.py' file in the 'update_data' folder has the Python function which ensures the current user is part of the group which has access to MODIFY a single row and column of data.  The 'main.py' file in the 'db_setup' folder has all the Python function for environment setup and cleanup.

Remove the 'dist' folder after you clone down the repo.  Next run the following command: 'pip install setuptools wheel'.  Update the 'setup.py' with your own details, and then run the following command to build a new Python whl file: 'python setup.py sdist bdist_wheel'.  After running this command it creates the following folders: 'uc_update_table_row_column_with_fgac.egg-info', 'dist', and 'build'.  The new Python whl can be found in the 'dist' folder.

# Output of the run_notebook_example Databricks notebook

![step1.png](/readme_images/step1.png)

![step2.jpg](/readme_images/step2.png)

![step3.jpg](/readme_images/step3.png)

![step4.jpg](/readme_images/step4.png)