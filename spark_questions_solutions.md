# Spark Solutions

## Employee Questions and Solutions

### Question 1
**Problem:** How to count the number of employees in each department using Spark?

**Solution:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Create Spark session
spark = SparkSession.builder.appName("EmployeeCountByDepartment").getOrCreate()

# Example data
data = [
    ("Alice", "HR"),
    ("Bob", "IT"),
    ("Charlie", "IT"),
    ("David", "Finance"),
    ("Eva", "Finance"),
    ("Frank", "Finance")
]

columns = ["EmployeeName", "Department"]

df = spark.createDataFrame(data, columns)

# Count employees per department
result = df.groupBy("Department").agg(count("*").alias("EmployeeCount"))
result.show()
```

**Example Output:**
```
+----------+--------------+
|Department|EmployeeCount |
+----------+--------------+
|HR        |1             |
|IT        |2             |
|Finance   |3             |
+----------+--------------+
```

---

### Question 2
**Problem:** How to find the highest salary in each department?

**Solution:**
```python
from pyspark.sql.functions import max

# Example data
salary_data = [
    ("Alice", "HR", 50000),
    ("Bob", "IT", 70000),
    ("Charlie", "IT", 80000),
    ("David", "Finance", 75000),
    ("Eva", "Finance", 72000),
    ("Frank", "Finance", 68000)
]

columns_salary = ["EmployeeName", "Department", "Salary"]

df_salary = spark.createDataFrame(salary_data, columns_salary)

# Find highest salary per department
highest_salary = df_salary.groupBy("Department").agg(max("Salary").alias("MaxSalary"))
highest_salary.show()
```

**Example Output:**
```
+----------+---------+
|Department|MaxSalary|
+----------+---------+
|HR        |50000    |
|IT        |80000    |
|Finance   |75000    |
+----------+---------+
```

---

### Question 3
**Problem:** How to filter employees earning more than 70,000?

**Solution:**
```python
# Filter condition
high_earners = df_salary.filter(df_salary["Salary"] > 70000)
high_earners.show()
```

**Example Output:**
```
+------------+----------+------+
|EmployeeName|Department|Salary|
+------------+----------+------+
|Charlie     |IT        |80000 |
|David       |Finance   |75000 |
+------------+----------+------+
```

---

**Reference Links:**
- [Data Engineer Interview Preparation Guide](https://medium.com/@nishasreedharan/data-engineer-interview-preparation-complete-guide-98a9d16f6889)
- [Complete Solution and Explanation](https://lnkd.in/gsYwUsDV)

---

**Quote:**  
*"Perseverance and consistency will beat raw talent any day"*

