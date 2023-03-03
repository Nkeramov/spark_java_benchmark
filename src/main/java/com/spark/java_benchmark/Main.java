package com.spark.java_benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Math.*;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static final String FS_SEPARATOR = FileSystems.getDefault().getSeparator();
    private static final Logger LOGGER = LogManager.getLogger(Main.class);
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String CSV_DELIMITER = ",";
    private static final String DATASET_NAME = "opensky";
    private static final String CSV_PATH = System.getProperty("user.home") + FS_SEPARATOR + DATASET_NAME;
    private static final String PARQUET_PATH = System.getProperty("user.home") + FS_SEPARATOR + DATASET_NAME +
            FS_SEPARATOR + DATASET_NAME + ".parquet";
    private static final int TEST_REPEAT_COUNT = 10;
    private static final int TEST_TYPES_COUNT = 5;
    private static final int WINDOW_SIZE_IN_DAYS = 30;
    private static final int DOUBLE_PRECISION = 5;
    private static final int EARTH_RADIUS = 6_371_000;
    private static final double DEG2RAD = 180 / PI;

    /**
     * createParquetDataset - read dataset from CSV files, duplicate data with time offset (3 and 6 years)
     * (to increase the amount of data), save dataset to parquet files
     *
     * @param spark      - current spark session
     * @param inputPath  - path to CSV files directory (input data)
     * @param outputPath - path to parquet files directory (output data)
     */
    static void createParquetDataset(SparkSession spark, String inputPath, String outputPath) {
        Map<String, String> optionsMap = new HashMap<>() {{
            put("delimiter", CSV_DELIMITER);
            put("header", "true");
            put("inferSchema", "true");
        }};
        Dataset<Row> df = spark.read().options(optionsMap).csv(inputPath)
                .withColumn("firstseen", to_date(col("firstseen"), TIMESTAMP_FORMAT))
                .withColumn("lastseen", to_timestamp(col("lastseen"), TIMESTAMP_FORMAT));
        Dataset<Row> df2 = df
                .withColumn("firstseen", col("firstseen").minus(expr("INTERVAL 3 YEARS")))
                .withColumn("lastseen", col("lastseen").minus(expr("INTERVAL 3 YEARS")));
        Dataset<Row> df3 = df
                .withColumn("firstseen", col("firstseen").minus(expr("INTERVAL 6 YEARS")))
                .withColumn("lastseen", col("lastseen").minus(expr("INTERVAL 6 YEARS")));
        Dataset<Row> df4 = df.union(df2).union(df3)
                .withColumn("firstseen", date_format(col("firstseen"), TIMESTAMP_FORMAT))
                .withColumn("lastseen", date_format(col("lastseen"), TIMESTAMP_FORMAT));
        df4.write().format("parquet").save(outputPath);
    }

    /**
     * readParquetDataset - read dataset from parquet files,
     * selects columns (origin, destination, firstseen, lastseen, latitude_1, longitude_1, latitude_2, longitude_2),
     * filters rows with nulls
     * casts columns firstseen, lastseen to timestamp type
     * casts columns latitude_1, longitude_1, latitude_2, longitude_2 to double type
     *
     * @param spark - current spark session
     * @param path  - path to parquet files directory (input data)
     * @return dataframe with selected rows
     */
    static Dataset<Row> readParquetDataset(SparkSession spark, String path) {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("recursiveFileLookup", "true");
        return spark.read().options(optionsMap).parquet(path)
                .select(col("origin"), col("destination"),
                        col("firstseen"), col("lastseen"),
                        col("latitude_1"), col("longitude_1"),
                        col("latitude_2"), col("longitude_2"))
                .filter(col("origin").isNotNull())
                .filter(col("destination").isNotNull())
                .filter(col("latitude_1").isNotNull())
                .filter(col("longitude_1").isNotNull())
                .filter(col("latitude_2").isNotNull())
                .filter(col("longitude_2").isNotNull())
                .withColumn("firstseen", to_timestamp(col("firstseen"), TIMESTAMP_FORMAT))
                .withColumn("lastseen", to_timestamp(col("lastseen"), TIMESTAMP_FORMAT))
                .withColumn("latitude_1", col("latitude_1").cast(DataTypes.DoubleType))
                .withColumn("longitude_1", col("longitude_1").cast(DataTypes.DoubleType))
                .withColumn("latitude_2", col("latitude_2").cast(DataTypes.DoubleType))
                .withColumn("longitude_2", col("longitude_2").cast(DataTypes.DoubleType));
    }

    /**
     * measureQueryExecutionTime - measures query execution time
     *
     * @param spark       - current spark session
     * @param parquetPath - path to parquet files directory (input data)
     * @param func        - query applied to a dataset
     * @return HashMap with query execution time and number of rows in the query result dataframe
     */
    static HashMap<String, Long> measureQueryExecutionTime(SparkSession spark, String parquetPath,
                                                           Function<Dataset<Row>, Dataset<Row>> func) {
        spark.catalog().clearCache();
        long start = System.nanoTime();
        Dataset<Row> df = func.apply(readParquetDataset(spark, parquetPath));
        long rowsCount = df.count();
        long finish = System.nanoTime();
        long executionTime = (finish - start) / (long) 1e9;
        return new HashMap<>() {{
            put("execution_time", executionTime);
            put("row_count", rowsCount);
        }};
    }

    public static void main(String[] args) {
        DecimalFormat decimalFormat = new DecimalFormat("###,###.#");
        System.setProperty("log4j2.configurationFile",
                String.valueOf(Main.class.getClassLoader().getResource("log4j2.xml")));
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.files.ignoreCorruptFiles", "true")
                .appName("SparkJavaTest")
                .master("local[8]")
                .config("spark.executor.memory", "2g")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        if (Files.isDirectory(Paths.get(CSV_PATH))) {
            LOGGER.info("Spark Java API benchmark");
            LOGGER.info("Query list:");
            LOGGER.info("1: reading with filtering");
            LOGGER.info("2: reading with aggregation");
            LOGGER.info("3: reading with aggregation and then filtering");
            LOGGER.info("4: reading and calculating the maximum of a complex function (as calculated column) in a sliding window");
            LOGGER.info("5: reading and calculating the maximum of a complex function (with UDF) in a sliding window");
            LOGGER.info(String.format("Number of repetitions of each query: %d", TEST_REPEAT_COUNT));
            if (!Files.isDirectory(Paths.get(PARQUET_PATH))) {
                LOGGER.info("Dataset preprocessing started ...");
                createParquetDataset(spark, CSV_PATH, PARQUET_PATH);
                LOGGER.info("Dataset increased in size and converted from csv to parquet files");
            }
            LOGGER.info("Dataset reading started ...");
            HashMap<String, Long> res = measureQueryExecutionTime(spark, PARQUET_PATH, Dataset::persist);
            long executionTime = res.getOrDefault("execution_time", 0L);
            long rowCount = res.getOrDefault("row_count", 0L);
            LOGGER.info(String.format("Elapsed time %s sec, selected %s rows", decimalFormat.format(executionTime),
                    decimalFormat.format(rowCount)));
            LOGGER.info(String.format("Dataset schema: \n%s",
                    readParquetDataset(spark, PARQUET_PATH).schema().treeString()));
            spark.catalog().clearCache();
            List<Function<Dataset<Row>, Dataset<Row>>> spark_queries = new ArrayList<>();
            spark_queries.add(df -> df
                    .filter(col("origin").isin("UUEE", "UUDD", "UUWW"))
                    .filter(to_date(col("firstseen"), "YYYY-MM-DD").geq(Date.valueOf("2020-06-01")))
                    .filter(to_date(col("firstseen"), "YYYY-MM-DD").leq(Date.valueOf("2020-08-31")))
            );
            spark_queries.add(df -> df
                    .withColumn("flighttime", col("lastseen").cast(DataTypes.LongType)
                            .minus(col("firstseen").cast(DataTypes.LongType)))
                    .groupBy(col("origin"), col("destination"))
                    .agg(avg("flighttime").alias("AvgFlightTime"))
                    .orderBy(col("AvgFlightTime").desc())
            );
            WindowSpec ws3 = Window.orderBy(monotonically_increasing_id())
                    .rowsBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            spark_queries.add(df -> df
                    .withColumn("flighttime", col("lastseen")
                            .cast(DataTypes.LongType).minus(col("firstseen").cast(DataTypes.LongType)))
                    .groupBy(col("origin"), col("destination"))
                    .agg(round(avg("flighttime"),
                            DOUBLE_PRECISION).alias("AvgFlightTime"))
                    .withColumn("TotalAvgFlightTime", round(
                            avg("AvgFlightTime").over(ws3).cast(DataTypes.DoubleType), DOUBLE_PRECISION))
                    .filter(col("AvgFlightTime").geq(col("TotalAvgFlightTime")))
                    .orderBy(col("AvgFlightTime").desc())
            );
            WindowSpec ws4 = Window.orderBy(col("flightdate"))
                    .rowsBetween(Window.currentRow() - WINDOW_SIZE_IN_DAYS, Window.currentRow());
            spark_queries.add(df -> df
                    .withColumn("distance_km", round(
                            lit(2 * EARTH_RADIUS).multiply(asin(sqrt(
                                    pow(sin((col("latitude_2").divide(lit(DEG2RAD))
                                            .minus(col("latitude_1").divide(lit(DEG2RAD))))
                                            .divide(lit(2.0))), 2.0)
                                            .plus(cos(col("latitude_1").divide(lit(DEG2RAD)))
                                                    .multiply(cos(col("latitude_2").divide(lit(DEG2RAD))))
                                                    .multiply(pow(
                                                            sin((col("longitude_2").divide(lit(DEG2RAD))
                                                                    .minus(col("longitude_1").divide(lit(DEG2RAD))))
                                                                    .divide(lit(2.0))), 2.0)
                                                    )
                                            )
                            ))), 0).divide(lit(1000.0)))
                    .withColumn("flightdate", to_date(col("firstseen")))
                    .groupBy(col("flightdate"))
                    .agg(max("distance_km").as("max_distance_km"))
                    .select(
                            col("flightdate"),
                            max(col("max_distance_km")).over(ws4).as("max_distance_km_on_date")
                    )
                    .orderBy(col("flightdate").asc())
            );
            UDF4<Double, Double, Double, Double, Double> get_distance =
                    (latitude_1, longitude_1, latitude_2, longitude_2) ->
                            round(2 * EARTH_RADIUS * asin(sqrt(pow(sin((toRadians(latitude_2) - toRadians(latitude_1)) / 2.0), 2.0)
                                    + cos(toRadians(latitude_1)) * cos(toRadians(latitude_2)) *
                                    pow(sin((toRadians(longitude_2) - toRadians(longitude_1)) / 2.0), 2.0)))) / 1000.0;
            spark.udf().register("get_distance", get_distance, DataTypes.DoubleType);
            spark_queries.add(df -> df
                    .withColumn("distance_km",
                            callUDF("get_distance", col("latitude_1"), col("longitude_1"),
                                    col("latitude_2"), col("longitude_2")
                            )
                    )
                    .withColumn("flightdate", to_date(col("firstseen")))
                    .groupBy(col("flightdate"))
                    .agg(max("distance_km").as("max_distance_km"))
                    .select(col("flightdate"), max(col("max_distance_km"))
                            .over(ws4).as("max_distance_km_on_date"))
                    .orderBy(col("flightdate").asc())
            );
            long[] executionTimes = new long[TEST_TYPES_COUNT];
            for (int i = 0; i < TEST_REPEAT_COUNT; i++) {
                LOGGER.info(String.format("%d iteration", i + 1));
                for (int j = 0; j < spark_queries.size(); j++) {
                    Function<Dataset<Row>, Dataset<Row>> spark_query = spark_queries.get(j);
                    res = measureQueryExecutionTime(spark, PARQUET_PATH, spark_query);
                    executionTime = res.getOrDefault("execution_time", 0L);
                    rowCount = res.getOrDefault("row_count", 0L);
                    LOGGER.info(String.format("\t%d: elapsed time %s sec, selected %s rows", j + 1,
                            decimalFormat.format(executionTime), decimalFormat.format(rowCount)));
                    executionTimes[j] += executionTime;
                }
            }
            LOGGER.info("Average query execution time");
            for (int i = 0; i < executionTimes.length; i++) {
                executionTimes[i] = round((double) executionTimes[i] / TEST_REPEAT_COUNT);
                LOGGER.info(String.format("\t%d: %s sec", i + 1, decimalFormat.format(executionTimes[i])));
            }
            LOGGER.info("All tests completed successfully");
        } else {
            LOGGER.error("Dataset files not found");
        }
        spark.stop();
    }
}