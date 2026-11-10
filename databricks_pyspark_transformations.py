
# ==========================================================
# PROJECT: DATABRICKS PYSPARK DATA TRANSFORMATIONS
# Description: Employee Data Transformation using PySpark
# ==========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==========================================================
# 1. CREATE SAMPLE DATASETS
# ==========================================================

employee_data = [
    (101, "Kaleem", "Data Engineering", 110000, 31, "2022-01-15"),
    (102, "Ali", "DevOps", 90000, 28, "2023-03-10"),
    (103, "Ahmed", "HR", 75000, 35, "2021-06-20"),
    (104, "Sara", "Data Engineering", None, 29, "2022-08-12"),
    (105, "John", "Finance", 85000, None, "2020-05-18"),
    (106, "David", "DevOps", 95000, 32, "2023-01-25"),
    (106, "David", "DevOps", 95000, 32, "2023-01-25"),
    (107, "Maria", "HR", 70000, 27, "2024-02-01"),
    (108, "Robert", "Finance", None, 40, "2021-09-15")
]

department_data = [
    ("Data Engineering", "Manager A", "Hyderabad"),
    ("DevOps", "Manager B", "Bangalore"),
    ("HR", "Manager C", "Mumbai"),
    ("Finance", "Manager D", "Pune")
]

employee_columns = [
    "employee_id", "name", "department",
    "salary", "age", "joining_date"
]

department_columns = [
    "department", "manager", "location"
]

employees_df = spark.createDataFrame(
    employee_data, employee_columns
)

departments_df = spark.createDataFrame(
    department_data, department_columns
)


# ==========================================================
# 2. DATA INSPECTION
# ==========================================================

print("===== ORIGINAL EMPLOYEE DATA =====")

employees_df.show()

employees_df.printSchema()

print("Total Records:", employees_df.count())


# ==========================================================
# 3. DATA CLEANING
# ==========================================================

# Remove duplicate employee records
cleaned_df = employees_df.dropDuplicates()

# Handle missing salary using median
median_salary = cleaned_df.approxQuantile(
    "salary", [0.5], 0.01
)[0]

cleaned_df = cleaned_df.fillna({
    "salary": median_salary,
    "age": 0
})

# Standardize employee names
cleaned_df = cleaned_df.withColumn(
    "name",
    F.initcap(F.trim(F.col("name")))
)

# Convert joining date into DateType
cleaned_df = cleaned_df.withColumn(
    "joining_date",
    F.to_date(F.col("joining_date"), "yyyy-MM-dd")
)

print("===== CLEANED DATA =====")

cleaned_df.show()


# ==========================================================
# 4. COLUMN TRANSFORMATIONS
# ==========================================================

transformed_df = (
    cleaned_df
    .withColumn(
        "annual_salary",
        F.col("salary") * 12
    )
    .withColumn(
        "annual_bonus",
        F.col("salary") * 0.10 * 12
    )
    .withColumn(
        "salary_category",
        F.when(F.col("salary") >= 100000, "High Salary")
         .otherwise("Standard Salary")
    )
    .withColumn(
        "joining_year",
        F.year(F.col("joining_date"))
    )
)

print("===== TRANSFORMED DATA =====")

transformed_df.show()


# ==========================================================
# 5. FILTERING AND SELECTING COLUMNS
# ==========================================================

high_salary_df = transformed_df.filter(
    F.col("salary") > 90000
)

selected_df = transformed_df.select(
    "employee_id",
    "name",
    "department",
    "salary",
    "annual_salary"
)

print("===== HIGH SALARY EMPLOYEES =====")

high_salary_df.show()


# ==========================================================
# 6. GROUP BY AND AGGREGATIONS
# ==========================================================

department_summary = transformed_df.groupBy(
    "department"
).agg(
    F.count("employee_id").alias("total_employees"),
    F.avg("salary").alias("average_salary"),
    F.max("salary").alias("maximum_salary"),
    F.min("salary").alias("minimum_salary"),
    F.sum("salary").alias("total_salary")
)

print("===== DEPARTMENT SUMMARY =====")

department_summary.show()


# ==========================================================
# 7. JOIN EMPLOYEE AND DEPARTMENT DATA
# ==========================================================

joined_df = transformed_df.join(
    departments_df,
    on="department",
    how="left"
)

print("===== JOINED DATA =====")

joined_df.show()


# ==========================================================
# 8. WINDOW FUNCTIONS
# ==========================================================

window_spec = Window.partitionBy(
    "department"
).orderBy(
    F.col("salary").desc()
)

window_df = joined_df.withColumn(
    "salary_rank",
    F.dense_rank().over(window_spec)
)

print("===== DEPARTMENT-WISE SALARY RANKING =====")

window_df.select(
    "name",
    "department",
    "salary",
    "salary_rank"
).show()


# ==========================================================
# 9. FINAL DATA VALIDATION
# ==========================================================

print("===== FINAL DATA VALIDATION =====")

print("Total Records:", window_df.count())

print("Missing Values:")

for column in window_df.columns:
    print(
        column,
        window_df.filter(F.col(column).isNull()).count()
    )


# ==========================================================
# 10. WRITE DATA INTO DELTA TABLE
# ==========================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS learning_db")

(
    window_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("learning_db.employee_transformed")
)

print("Data successfully saved into Delta table!")


# ==========================================================
# 11. READ TRANSFORMED DELTA DATA
# ==========================================================

final_df = spark.table(
    "learning_db.employee_transformed"
)

print("===== FINAL DELTA TABLE =====")

final_df.show()

print("PySpark transformations completed successfully!")
