from setuptools import setup, find_packages

setup(
    name='uc_update_table_row_column_with_fgac',
    version='0.1',
    packages=find_packages(),
    description='Python distribution utilities',
    long_description=open('README.md').read(),
    install_requires=[
        # List your dependencies here
        # e.g., 'requests', 'numpy',
    ],
    python_requires='>=3.6',
    author='Robert Altmiller',
    author_email='robert.altmiller@databricks.com'
)