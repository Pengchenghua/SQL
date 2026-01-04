# SQL性能优化最佳实践指南

## 1. 查询优化基础

### 1.1 避免SELECT *
```sql
-- ❌ 不推荐
SELECT * FROM table_name;

-- ✅ 推荐
SELECT id, name, created_date FROM table_name;
```

### 1.2 使用EXISTS替代IN
```sql
-- ❌ 不推荐
SELECT * FROM table1 WHERE column1 IN (SELECT column2 FROM table2);

-- ✅ 推荐
SELECT * FROM table1 t1 WHERE EXISTS (SELECT 1 FROM table2 t2 WHERE t2.column2 = t1.column1);
```

### 1.3 合理使用索引
```sql
-- 创建复合索引
CREATE INDEX idx_name_date ON table_name (name, created_date);

-- 避免在索引列上使用函数
-- ❌ 不推荐
SELECT * FROM table WHERE YEAR(created_date) = 2024;

-- ✅ 推荐
SELECT * FROM table WHERE created_date >= '2024-01-01' AND created_date < '2025-01-01';
```

## 2. JOIN优化策略

### 2.1 JOIN顺序优化
```sql
-- 小表驱动大表原则
-- ❌ 不推荐（大表在前）
SELECT * FROM large_table l JOIN small_table s ON l.id = s.id;

-- ✅ 推荐（小表在前）
SELECT * FROM small_table s JOIN large_table l ON s.id = l.id;
```

### 2.2 使用INNER JOIN替代WHERE
```sql
-- ❌ 不推荐
SELECT * FROM table1, table2 WHERE table1.id = table2.id;

-- ✅ 推荐
SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id;
```

## 3. 聚合函数优化

### 3.1 减少GROUP BY字段数量
```sql
-- ❌ 不推荐（过多分组字段）
SELECT a,b,c,d,e,f, SUM(value) 
FROM table 
GROUP BY a,b,c,d,e,f;

-- ✅ 推荐（合理分组字段）
SELECT a,b, SUM(value) 
FROM table 
GROUP BY a,b;
```

### 3.2 使用窗口函数替代复杂聚合
```sql
-- 原复杂聚合
SELECT dept, year, SUM(sales),
       (SELECT SUM(sales) FROM sales s2 WHERE s2.dept = s1.dept) as dept_total
FROM sales s1
GROUP BY dept, year;

-- 使用窗口函数
SELECT dept, year, SUM(sales) OVER(PARTITION BY dept, year) as year_sales,
       SUM(sales) OVER(PARTITION BY dept) as dept_total
FROM sales;
```

## 4. 临时表优化

### 4.1 减少临时表数量
```sql
-- ❌ 不推荐：多个临时表
CREATE TEMPORARY TABLE temp1 AS SELECT ...;
CREATE TEMPORARY TABLE temp2 AS SELECT ... FROM temp1;
CREATE TEMPORARY TABLE temp3 AS SELECT ... FROM temp2;

-- ✅ 推荐：使用CTE或子查询
WITH temp1 AS (SELECT ...),
     temp2 AS (SELECT ... FROM temp1)
SELECT * FROM temp2;
```

### 4.2 合理使用物化视图
```sql
-- 创建物化视图提升常用聚合查询性能
CREATE MATERIALIZED VIEW mv_sales_summary AS
SELECT region, product, SUM(amount) as total_sales
FROM sales
GROUP BY region, product;
```

## 5. 分区和分桶策略

### 5.1 时间分区
```sql
-- 按时间分区
CREATE TABLE sales_partitioned (
    id INT,
    sale_date DATE,
    amount DECIMAL(10,2)
) PARTITIONED BY (sale_year INT, sale_month INT);

-- 插入数据时指定分区
INSERT INTO sales_partitioned PARTITION(sale_year=2024, sale_month=12)
SELECT id, sale_date, amount FROM sales_source;
```

### 5.2 数据分桶
```sql
-- 按关键字段分桶
CREATE TABLE sales_bucketed (
    id INT,
    customer_id INT,
    amount DECIMAL(10,2)
) CLUSTERED BY (customer_id) INTO 10 BUCKETS;
```

## 6. Hive特定优化

### 6.1 向量化执行
```sql
-- 启用向量化执行
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
```

### 6.2 数据倾斜处理
```sql
-- 处理数据倾斜
SET hive.groupby.skewindata=true;
SET hive.optimize.skewjoin=true;

-- 使用MAP JOIN处理小表
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;
```

### 6.3 合理设置Reducer数量
```sql
-- 根据数据量设置Reducer数量
SET mapred.reduce.tasks=合适的数量;
-- 或者让Hive自动决定
SET hive.exec.reducers.bytes.per.reducer=256000000;
```

## 7. 代码结构和可维护性

### 7.1 使用CTE提高可读性
```sql
WITH base_data AS (
    SELECT * FROM source_table WHERE date >= '2024-01-01'
),
aggregated_data AS (
    SELECT region, SUM(sales) as total_sales
    FROM base_data
    GROUP BY region
)
SELECT * FROM aggregated_data ORDER BY total_sales DESC;
```

### 7.2 统一的命名规范
```sql
-- 临时表命名规范
csx_analyse_tmp.table_purpose_date
-- 例如：csx_analyse_tmp.sales_summary_202412

-- 字段命名规范
snake_case_naming
-- 例如：total_sales_amount, customer_count
```

## 8. 性能监控和分析

### 8.1 执行计划分析
```sql
-- 查看执行计划
EXPLAIN 
SELECT * FROM table WHERE conditions;

-- 查看详细执行计划
EXPLAIN EXTENDED 
SELECT * FROM table WHERE conditions;
```

### 8.2 性能监控查询
```sql
-- 查看表大小和分区信息
SHOW PARTITIONS table_name;
DESCRIBE FORMATTED table_name;

-- 查看查询统计信息
SET hive.compute.query.using.stats=true;
ANALYZE TABLE table_name COMPUTE STATISTICS;
```

## 9. 实际案例优化

### 案例：供应链采购分析优化
```sql
-- 原查询（性能问题）
-- 多次扫描源表，重复GROUPING SETS

-- 优化后查询
WITH base_aggregation AS (
    -- 单次基础聚合
    SELECT dimensions, measures, time_periods
    FROM source_table
    WHERE conditions
    GROUP BY dimensions
),
multi_dimensional AS (
    -- 多维分析统一处理
    SELECT *,
           CASE WHEN level_conditions THEN 'level_name' END as hierarchy
    FROM base_aggregation
)
SELECT hierarchy, SUM(measures)
FROM multi_dimensional
GROUP BY hierarchy;
```

## 10. 持续优化建议

### 定期维护
- 每周清理过期临时表
- 每月更新统计信息
- 每季度审查索引策略

### 性能监控
- 建立查询性能基线
- 监控长耗时查询
- 设置性能告警阈值

### 团队协作
- 建立代码审查流程
- 分享性能优化经验
- 定期培训SQL最佳实践