
# HadoopWithHive

## Assignment Tasks:

### Data Preparation
- Download and extract the GHCNd data for the year 2023.

### Hive Table Creation
- Design and create a Hive table suitable for the GHCNd data, considering the most effective partitioning strategy for efficient querying.

### Data Loading
- Import the GHCNd data into the created Hive table.

### Data Organization Inspection
- Utilize Hadoop's WebUI to explore the Hive table's underlying directory and file organization in HDFS.

### Temperature Analysis
- Develop a HiveQL query to compute the average daily maximum and minimum temperatures for a specified month.

### Precipitation Data Analysis
- Formulate a query to identify the count of stations with missing precipitation data.

### Advanced Analysis Using Window Functions
- Apply window functions in HiveQL for advanced data analysis, focusing on a specific analytical goal.

### Testing and Validation
- Test your HiveQL queries using the 2023 GHCNd dataset. Optionally, validate your queries with datasets from additional years for comparative analysis.

### Report Compilation
- Draft a comprehensive report detailing your methodology, partitioning strategy, data directory structure, encountered challenges, and include the HiveQL code utilized for the analyses.

## Steps:

1. Download the 2023.csv dataset from the GHCNd.
2. Push to Github codespaces.
3. Unzip using `gunzip 2023.csv.gz`.
4. Copy this 2023.csv into the Docker resource manager for storing into the Hadoop cluster: `docker cp 2023.csv resourcemanager:/tmp`.
5. Connect to the Docker resource manager: `docker exec -it resourcemanager /bin/bash`, then navigate to `/tmp`.
6. Before copying, create a folder in Hadoop: `hadoop fs -mkdir -p user/root/input`.
7. Copy the file from the local file system to the Hadoop file system: `hadoop fs -put 2023.csv user/root/input`.
8. Cross-verify if it's copied into Hadoop FS by: `hadoop fs -ls user/root/input/*`.
9. Create tables using Hive and push the dataset into a table.
10. Connect to the Hive Docker service: `docker exec -it hive-pig /bin/bash`.
11. Connect to Hive: `hive`.
12. Create a new database using: `CREATE DATABASE aravind`.
13. Use this new database: `USE aravind`.
14. Create a staging table as an intermediate table for reading raw data from CSV and then load it into this temporary staging table.

```sql
CREATE TABLE weather_data_staging (
    station_id STRING,
    date_str STRING,
    metric_type STRING,
    metric_value INT,
    metric_flag STRING,
    quality_flag STRING,
    source_flag STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

Then also create a schema for the final table.

```sql
CREATE TABLE weather_data (
    station_id STRING,
    record_time_date DATE,
    metric_type STRING,
    metric_value INT,
    metric_flag STRING,
    quality_flag STRING,
    source_flag STRING
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

15. Now schemas for both the staging table and final table are ready. Let's try to load raw data into the staging table.

```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
```

Load data into the staging table:

```sql
LOAD DATA INPATH 'user/root/input/2023.csv' INTO TABLE weather_data_staging;
```

16. Now let's cross-verify if records are inserted into the table:

```sql
SELECT COUNT(*) FROM weather_data_staging;
```

17. Now let's try to insert records into the final table by changing the months and year fields in the WHERE clause.

18. Inserting all records at once instead of individual batch operations.

```sql
INSERT INTO TABLE weather_data PARTITION (year, month)
SELECT 
    station_id,
    from_unixtime(unix_timestamp(date_str, 'yyyyMMdd'), 'yyyy-MM-dd') AS record_time_date,
    metric_type,
    metric_value,
    metric_flag,
    quality_flag,
    source_flag,
    year(from_unixtime(unix_timestamp(date_str, 'yyyyMMdd'))) AS year,
    month(from_unixtime(unix_timestamp(date_str, 'yyyyMMdd'))) AS month
FROM 
    weather_data_staging
WHERE 
    month(from_unixtime(unix_timestamp(date_str, 'yyyyMMdd'))) = 1
    AND year(from_unixtime(unix_timestamp(date_str, 'yyyyMMdd'))) = 2023;
```

19. Cross-verify if records are inserted into the final table:

```sql
SELECT COUNT(*) FROM weather_data;
```

20. Finding out the average of maximum and minimum temperatures by grouping columns:

```sql
SELECT 
    record_time_date, 
    AVG(CASE WHEN metric_type = 'TMAX' THEN metric_value END) AS avg_max_temp, 
    AVG(CASE WHEN metric_type = 'TMIN' THEN metric_value END) AS avg_min_temp
FROM 
    weather_data
GROUP BY 
    record_time_date
ORDER BY 
    record_time_date;
```

21. Same query but with a smaller operation:

```sql
SELECT 
    record_time_date, 
    AVG(CASE WHEN metric_type = 'TMAX' THEN metric_value END) AS avg_max_temp, 
    AVG(CASE WHEN metric_type = 'TMIN' THEN metric_value END) AS avg_min_temp
FROM 
    weather_data
WHERE 
    month = 1 AND year = 2023
GROUP BY 
    record_time_date
ORDER BY 
    record_time_date;
```

22. Finding out station IDs which don't have precipitation or zero:

```sql
SELECT DISTINCT station_id
FROM weather_data wd1
WHERE NOT EXISTS (
    SELECT 1
    FROM weather_data wd2
    WHERE wd2.metric_type = 'PRCP'
    AND wd1.station_id = wd2.station_id
);
```

23. The query looks at daily temperature records for a specific weather station and calculates an average temperature for each day based on that day's temperature and the temperatures of the three days before and after it. This helps see if the temperature trend at that station is going up, going down, or staying about the same over a week. It's like smoothing out the daily ups and downs to get a clearer picture of the temperature trend.

 ```sql
SELECT station_id, record_date, metric_type, 
       AVG(metric_value) OVER (PARTITION BY station_id, metric_type ORDER BY record_date 
       ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) as moving_avg_temp
FROM weather_data
WHERE metric_type IN ('TMAX', 'TMIN') AND station_id = 'SpecificStationID';
```
