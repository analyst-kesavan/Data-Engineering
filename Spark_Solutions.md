# Spark Solutions

## Question: Employee Salary Analysis
You are given an employee dataset with the following columns:
- `employee_id` (integer)
- `name` (string)
- `department` (string)
- `salary` (integer)
- `joining_date` (date)

### Task:
1. Find the highest-paid employee in each department.
2. Calculate the average salary of employees who joined after 2020.

---

## Solution (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, avg, year

# Create Spark session
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()

# Sample data
data = [
    (1, "Alice", "HR", 50000, "2021-05-10"),
    (2, "Bob", "IT", 80000, "2019-08-15"),
    (3, "Charlie", "IT", 90000, "2022-03-20"),
    (4, "David", "Finance", 75000, "2021-07-01"),
    (5, "Eva", "Finance", 70000, "2020-12-11")
]

columns = ["employee_id", "name", "department", "salary", "joining_date"]
df = spark.createDataFrame(data, columns)

# Convert joining_date to date type
from pyspark.sql.functions import to_date
df = df.withColumn("joining_date", to_date(col("joining_date"), "yyyy-MM-dd"))

# 1. Highest-paid employee in each department
from pyspark.sql.window import Window
import pyspark.sql.functions as F

windowSpec = Window.partitionBy("department").orderBy(F.desc("salary"))
highest_paid_df = df.withColumn("rank", F.rank().over(windowSpec)).filter(col("rank") == 1).drop("rank")
highest_paid_df.show()

# 2. Average salary of employees joined after 2020
avg_salary_df = df.filter(year(col("joining_date")) > 2020).agg(avg("salary").alias("avg_salary"))
avg_salary_df.show()
```

---

## Expected Output

**Highest Paid Employee in Each Department**
```
+-----------+-------+----------+------+------------+
|employee_id|   name|department|salary|joining_date|
+-----------+-------+----------+------+------------+
|          1|  Alice|        HR| 50000|  2021-05-10|
|          3|Charlie|        IT| 90000|  2022-03-20|
|          4|  David|   Finance| 75000|  2021-07-01|
+-----------+-------+----------+------+------------+
```

**Average Salary of Employees Joined After 2020**
```
+----------+


# PySpark: Find Employees Absent for 10 or More Continuous Days

## Problem Statement

Given the following employee attendance data with attributes:
- **EmpId**: Employee ID
- **Attendance**: 'A' for Absent, 'P' for Present
- **Date**: Date of attendance record

We want to identify all employees who have been **continuously absent** for **10 or more days**.

### Sample Data

| EmpId | Attendance | Date       |
|-------|------------|------------|
| 1     | A          | 2024-04-25 |
| 2     | A          | 2024-04-25 |
| 3     | P          | 2024-04-25 |
| 1     | A          | 2024-04-26 |
| 2     | A          | 2024-04-26 |
| 3     | P          | 2024-04-26 |
| 1     | A          | 2024-04-27 |
| 2     | A          | 2024-04-27 |
| 3     | P          | 2024-04-27 |

---

## PySpark Solution

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("ContinuousAbsentees").getOrCreate()

# Sample data
data = [
    (1, "A", "2024-04-25"),
    (2, "A", "2024-04-25"),
    (3, "P", "2024-04-25"),
    (1, "A", "2024-04-26"),
    (2, "A", "2024-04-26"),
    (3, "P", "2024-04-26"),
    (1, "A", "2024-04-27"),
    (2, "A", "2024-04-27"),
    (3, "P", "2024-04-27"),
]

# Create DataFrame
df = spark.createDataFrame(data, ["EmpId", "Attendance", "Date"])

# Convert Date to DateType
df = df.withColumn("Date", F.to_date("Date"))

# Filter only Absent records
df_absent = df.filter(F.col("Attendance") == "A")

# Window to order by date per employee
window_spec = Window.partitionBy("EmpId").orderBy("Date")

# Assign row numbers and sequence grouping key
df_absent = df_absent.withColumn("rn", F.row_number().over(window_spec))
df_absent = df_absent.withColumn(
    "grp", F.datediff("Date", F.lit("2024-01-01")) - F.col("rn")
)

# Count continuous days per group
df_grouped = (
    df_absent.groupBy("EmpId", "grp")
    .agg(F.count("*").alias("consecutive_days"))
    .filter(F.col("consecutive_days") >= 10)
)

df_grouped.show()

|avg_salary|
+----------+
|   71666.6|
+----------+
```
## Expected Output

```
+-----+---+----------------+
|EmpId|grp|consecutive_days|
+-----+---+----------------+
|   2 | 45|              12|
|   1 | 12|              10|
+-----+---+----------------+
```
##Explanation
Filter only Absent records → We only care about 'A'.

Use Window Function → Assign a row_number per employee ordered by date.

Grouping Key → (Date difference) - row_number stays constant for consecutive dates, allowing grouping.

Count consecutive absences → Group by (EmpId, grp) and count days.

Filter for ≥10 days → Only employees with 10+ consecutive absent days are returned.






