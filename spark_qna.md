# Apache Spark Interview Questions and Answers

## 1. Discuss the significance of checkpointing in Spark Streaming. When and why should you use it?

Checkpointing in Spark Streaming is the process of saving the state of a streaming application to a reliable storage (e.g., HDFS).
**When to use:**
- Long-running stateful transformations (updateStateByKey, mapWithState).
- To recover from failures.
**Why:** It ensures fault tolerance by allowing the application to recover from the saved state instead of recomputing everything from scratch.
Example: Saving offsets of Kafka streams to HDFS so that after a crash, processing resumes from the last checkpoint.

## 2. Explain the role of DAG (Directed Acyclic Graph) in Spark's execution model. How does it optimize task execution?

DAG represents the sequence of computations in Spark. Spark creates a logical plan in the form of a DAG, then optimizes and converts it into a physical execution plan.
**Optimization:** By understanding dependencies, Spark can group transformations into stages, reduce shuffles, and minimize execution time.
Example: If two map transformations occur before a shuffle, Spark can combine them into a single stage.

## 3. Discuss the use of window functions in Spark SQL. Provide examples of scenarios where window functions are beneficial.

Window functions allow performing calculations across a group of rows related to the current row.
**Use cases:**
- Calculating running totals.
- Ranking records.
Example: `row_number() OVER (PARTITION BY dept ORDER BY salary DESC)` finds top salaries per department.

## 4. Explain the stages of execution in Spark.

Stages in Spark execution:
1. **Job**: Triggered by an action (e.g., collect(), count()).
2. **Stage**: Division based on shuffle boundaries.
3. **Task**: Smallest unit of execution, sent to executors.
Example: A filter followed by a reduceByKey will create two stages—before and after the shuffle.

## 5. Discuss the Catalyst optimizer in Spark SQL. How does it optimize query execution?

Catalyst Optimizer is Spark SQL's query optimizer.
**Optimizations include:**
- Constant folding (e.g., replacing 2+3 with 5).
- Predicate pushdown (filtering data early).
- Reordering joins for better performance.
Example: Optimizing `SELECT * FROM table WHERE id = 10` by filtering before reading unnecessary data.

## 6. Describe how Spark handles fault tolerance

Spark handles fault tolerance through:
- **RDD lineage**: Recomputing lost partitions from source data.
- **Checkpointing**: Saving state to persistent storage.
- **Data replication** in cluster managers like YARN or Kubernetes.

## 7. Explain the concept of lazy evaluation in Spark. Why is it beneficial?

Lazy evaluation means Spark waits until an action is called before executing transformations.
**Benefits:**
- Optimizes execution by combining transformations.
- Avoids unnecessary computations.
Example: Multiple map() calls will be combined into one stage when an action like collect() is invoked.

## 8. What are the different types of data partitioning available in Spark? When would you use each?

Types of partitioning in Spark:
1. **Hash partitioning**: Based on a hash function.
2. **Range partitioning**: Based on sorting and ranges.
3. **Custom partitioning**: User-defined logic.
Use hash for joins, range for sorted data processing.

## 9. Discuss the differences between transformations and actions in Spark. Provide examples of each.

Transformations: Create a new RDD/DataFrame (lazy).
Example: map(), filter(), flatMap().
Actions: Trigger execution and return results.
Example: collect(), count(), saveAsTextFile().

## 10. Explain the process of data skewness and its impact on Spark jobs. How can you handle skewed data?

Data skew occurs when some partitions have disproportionately large amounts of data.
**Impact:** Slow tasks, imbalanced workload.
**Handling:**
- Salting keys in joins.
- Increasing parallelism.
- Broadcasting small datasets for joins.

