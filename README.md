
# Databricks PySpark Data Transformations

## About This Project

This project demonstrates practical data transformation
techniques using PySpark in Azure Databricks.

The project uses sample employee and department datasets
to perform data cleaning, transformation, aggregation,
joins, window functions, and Delta Lake operations.

## Technologies Used

- Azure Databricks
- Apache Spark
- PySpark
- Python
- Delta Lake
- Spark SQL

## Project Structure

databricks-pyspark-transformations/
│
├── README.md
│
└── databricks_pyspark_transformations.py

## Transformations Covered

1. DataFrame Creation and Inspection
2. Data Cleaning and Deduplication
3. Null Value Handling
4. Data Type Conversion
5. Column Transformations
6. Filtering and Column Selection
7. GroupBy and Aggregations
8. Inner and Left Joins
9. Window Functions
10. Data Validation
11. Delta Table Creation and Reading

## Project Workflow

Source Data
    |
    v
DataFrame Creation
    |
    v
Data Cleaning
    |
    v
Data Transformation
    |
    v
Filtering and Aggregation
    |
    v
Department Join
    |
    v
Window Functions
    |
    v
Data Validation
    |
    v
Delta Lake Table

## How to Run

1. Create an Azure Databricks workspace.
2. Create a Python notebook.
3. Copy the code into the notebook.
4. Attach a running compute cluster.
5. Execute the notebook cells.
6. Verify the transformed data.
7. Check the Delta table:

SELECT *
FROM learning_db.employee_transformed;

## Output

The project generates:

- Cleaned employee records
- Department-wise salary summary
- Employee salary ranking
- Final transformed Delta table

## Learning Objectives

- Understand PySpark DataFrames.
- Perform scalable data transformations.
- Handle duplicates and missing values.
- Implement joins and aggregations.
- Apply Spark window functions.
- Store transformed data using Delta Lake.

## Author

Mohd Kaleem

Azure Data Engineer | Azure DevOps Engineer

Continuously learning and building practical
Data Engineering projects using Azure Databricks,
PySpark, SQL, Python, and Azure services.

---

This repository is part of my Data Engineering
learning and practical implementation journey.
