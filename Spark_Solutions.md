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
|avg_salary|
+----------+
|   71666.6|
+----------+
```
