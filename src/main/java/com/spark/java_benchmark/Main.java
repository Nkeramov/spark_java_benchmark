package com.spark.java_benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Date;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


public class Main {
	private static final Logger LOGGER = LogManager.getLogger(Main.class);
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final String CSV_DELIMITER = ",";

	private static final String CSV_PATH = "/home/nizam/opensky/";
	private static final String PARQUET_PATH = "/home/nizam/opensky/opensky.parquet";
	private static final int TEST_REPEAT_COUNT = 10;
	private static final int TEST_TYPES_COUNT = 5;
	private static final int WINDOW_SIZE_IN_DAYS = 30;
	private static final int DOUBLE_PRECISION = 5;
	private static final int EARTH_RADIUS = 6_371_000;
	private static final double DEG2RAD = 180 / Math.PI;

	/**
	 * preprocessParquetDataset - read dataset from CSV files, duplicate data with time offset (3 and 6 years)
	 * (to increase the amount of data), save dataset to parquet files
	 * @param spark - current spark session
	 * @param inputPath - path to CSV files directory (input data)
	 * @param outputPath - path to parquet files directory (output data)
	 */
	public static void preprocessParquetDataset(SparkSession spark, String inputPath, String outputPath) {
		Map<String, String> optionsMap = new HashMap<>() {{
			put("delimiter", CSV_DELIMITER);
			put("header", "true");
			put("inferSchema", "true");
		}};
		Dataset<Row> opensky = spark.read().options(optionsMap).csv(inputPath)
				.withColumn("firstseen", functions.to_date(functions.col("firstseen"), TIMESTAMP_FORMAT))
				.withColumn("lastseen", functions.to_timestamp(functions.col("lastseen"), TIMESTAMP_FORMAT));
		Dataset<Row> opensky2 = opensky
				.withColumn("firstseen", functions.col("firstseen").minus(functions.expr("INTERVAL 3 YEARS")))
				.withColumn("lastseen", functions.col("lastseen").minus(functions.expr("INTERVAL 3 YEARS")));
		Dataset<Row> opensky3 = opensky
				.withColumn("firstseen", functions.col("firstseen").minus(functions.expr("INTERVAL 6 YEARS")))
				.withColumn("lastseen", functions.col("lastseen").minus(functions.expr("INTERVAL 6 YEARS")));
		Dataset<Row> opensky4 = opensky.union(opensky2).union(opensky3)
				.withColumn( "firstseen", functions.date_format(functions.col("firstseen"), TIMESTAMP_FORMAT))
				.withColumn( "lastseen", functions.date_format(functions.col("lastseen"), TIMESTAMP_FORMAT));
		opensky4.write().format("parquet").save(outputPath);
	}

	/**
	 * readParquetDataset - read dataset from parquet files,
	 * selects columns (origin, destination, firstseen, lastseen, latitude_1, longitude_1, latitude_2, longitude_2),
	 * filters rows with nulls
	 * casts columns firstseen, lastseen to timestamp type
	 * casts columns latitude_1, longitude_1, latitude_2, longitude_2 to double type,
	 * @param spark - current spark session
	 * @param path - path to parquet files directory (input data)
	 * @return dataframe with selected rows
	 */
	public static Dataset<Row> readParquetDataset(SparkSession spark, String path){
		Map<String, String> optionsMap = new HashMap<>();
		optionsMap.put("recursiveFileLookup", "true");
		return spark.read().options(optionsMap).parquet(path)
				.select(functions.col("origin"), functions.col("destination"),
						functions.col("firstseen"), functions.col("lastseen"),
						functions.col("latitude_1"), functions.col("longitude_1"),
						functions.col("latitude_2"), functions.col("longitude_2"))
				.filter(functions.col("origin").isNotNull())
				.filter(functions.col("destination").isNotNull())
				.filter(functions.col("latitude_1").isNotNull())
				.filter(functions.col("longitude_1").isNotNull())
				.filter(functions.col("latitude_2").isNotNull())
				.filter(functions.col("longitude_2").isNotNull())
				.withColumn("firstseen", functions.to_timestamp(functions.col("firstseen"), TIMESTAMP_FORMAT))
				.withColumn("lastseen", functions.to_timestamp(functions.col("lastseen"), TIMESTAMP_FORMAT))
				.withColumn("latitude_1", functions.col("latitude_1").cast(DataTypes.DoubleType))
				.withColumn("longitude_1", functions.col("longitude_1").cast(DataTypes.DoubleType))
				.withColumn("latitude_2", functions.col("latitude_2").cast(DataTypes.DoubleType))
				.withColumn("longitude_2", functions.col("longitude_2").cast(DataTypes.DoubleType));
	}

	public static String withLargeIntegers(double value) {
		DecimalFormat df = new DecimalFormat("###,###,###");
		return df.format(value);
	}

	/**
	 * measureQueryExecutionTime - measures query execution time
	 * @param spark - current spark session
	 * @param parquetPath - path to parquet files directory (input data)
	 * @param func - query applied to a dataset
	 * @return hashmap with query execution time and number of records in the query result dataframe
	 */
	public static HashMap<String, Long> measureQueryExecutionTime(SparkSession spark, String parquetPath,
																  Function<Dataset<Row>, Dataset<Row>> func) {
		spark.catalog().clearCache();
		long start = System.nanoTime();
		Dataset<Row> df = func.apply(readParquetDataset(spark, parquetPath));
		long recordsCount = df.count();
		long finish = System.nanoTime();
		long executionTime = (finish - start) / (long) 1e6;
		return new HashMap<>() {{
			put("execution_time", executionTime);
			put("records_count", recordsCount);
		}};
	}

	public static void main(String[] args) {
		System.setProperty("log4j2.configurationFile",
				String.valueOf(Main.class.getClassLoader().getResource("log4j2.xml")));
		LOGGER.info("Spark Java API benchmark");
		LOGGER.info("Query list:");
		LOGGER.info("\t1) reading with filtering");
		LOGGER.info("\t2) reading with aggregation");
		LOGGER.info("\t3) reading with aggregation and then filtering");
		LOGGER.info("\t4) reading and calculating the maximum of a complex function (as calculated column) in a sliding window");
		LOGGER.info("\t5) reading and calculating the maximum of a complex function (with UDF) in a sliding window");
		long[] testTimes = new long[TEST_TYPES_COUNT];
		long start, finish, executionTime, recordsCount;
		LOGGER.info(String.format("Number of repetitions of each query: %d", TEST_REPEAT_COUNT));
		LOGGER.info("Dataset reading test started ...");
		SparkSession spark = SparkSession.builder()
				.config("spark.sql.files.ignoreCorruptFiles", "true")
				.appName("SparkJavaTest")
				.master("local[8]")
				.config("spark.executor.memory", "2g")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		//preprocessParquetDataset(spark, csvPath, parquetPath);
		start = System.nanoTime();
		Dataset<Row> opensky = readParquetDataset(spark, PARQUET_PATH).persist();
		recordsCount = opensky.count();
		finish = System.nanoTime();
		executionTime = (finish - start) / (long) 1e6;
		LOGGER.info(String.format("Elapsed time %s ms, selected %s records", withLargeIntegers(executionTime), withLargeIntegers(recordsCount)));
		LOGGER.info("Dataset schema");
		LOGGER.info(opensky.schema().toString());
		//opensky.printSchema();
		spark.catalog().clearCache();
		List<Function<Dataset<Row>, Dataset<Row>>> spark_queries = new ArrayList<>();
		spark_queries.add(df -> df
			.filter(functions.col("origin").isin("UUEE", "UUDD", "UUWW"))
			.filter(functions.to_date(functions.col("firstseen"), "YYYY-MM-DD").geq(Date.valueOf("2020-06-01")))
			.filter(functions.to_date(functions.col("firstseen"), "YYYY-MM-DD").leq(Date.valueOf("2020-08-31")))
			.persist()
		);
		spark_queries.add(df -> df
			.withColumn("flighttime", functions.col("lastseen").cast(DataTypes.LongType)
				.minus(functions.col("firstseen").cast(DataTypes.LongType)))
			.groupBy(functions.col("origin"), functions.col("destination"))
			.agg(functions.avg("flighttime").alias("AvgFlightTime"))
			.orderBy(functions.col("AvgFlightTime").desc()).persist()
		);
		WindowSpec ws3 = Window.orderBy(functions.monotonically_increasing_id())
			.rowsBetween(Long.MIN_VALUE, Long.MAX_VALUE);
		spark_queries.add(df -> df
			.withColumn("flighttime", functions.col("lastseen")
					.cast(DataTypes.LongType).minus(functions.col("firstseen").cast(DataTypes.LongType)))
			.groupBy(functions.col("origin"), functions.col("destination"))
			.agg(functions.round(functions.avg("flighttime"),
					DOUBLE_PRECISION).alias("AvgFlightTime"))
			.withColumn("TotalAvgFlightTime", functions.round(
					functions.avg("AvgFlightTime").over(ws3).cast(DataTypes.DoubleType), DOUBLE_PRECISION))
			.filter(functions.col("AvgFlightTime").geq(functions.col("TotalAvgFlightTime")))
			.orderBy(functions.col("AvgFlightTime").desc()).persist()
		);
		WindowSpec ws4 = Window.orderBy(functions.col("flightdate"))
			.rowsBetween(Window.currentRow() - WINDOW_SIZE_IN_DAYS, Window.currentRow());
		spark_queries.add(df -> df
			.withColumn("distance_km", functions.round(
				functions.lit(2 * EARTH_RADIUS)
					.multiply(functions.asin(
						functions.sqrt(
							functions.pow(functions.sin((functions.col("latitude_2").divide(functions.lit(DEG2RAD))
											.minus(functions.col("latitude_1").divide(functions.lit(DEG2RAD))))
											.divide(functions.lit(2.0))), 2.0)
							.plus(functions.cos(functions.col("latitude_1").divide(functions.lit(DEG2RAD)))
								.multiply(functions.cos(functions.col("latitude_2").divide(functions.lit(DEG2RAD))))
								.multiply(functions.pow(
									functions.sin((functions.col("longitude_2").divide(functions.lit(DEG2RAD))
										.minus(functions.col("longitude_1").divide(functions.lit(DEG2RAD))))
									.divide(functions.lit(2.0))), 2.0)
								)
							)
						)
					)
				), 0).divide(functions.lit(1000.0)))
			.withColumn("flightdate", functions.to_date(functions.col("firstseen")))
			.groupBy(functions.col("flightdate"))
			.agg(functions.max("distance_km").as("max_distance_km"))
			.select(
				functions.col("flightdate"),
				functions.max(functions.col("max_distance_km")).over(ws4).as("max_distance_km_on_date")
			)
			.orderBy(functions.col("flightdate").asc()).persist()
		);
		UDF4<Double, Double, Double, Double, Double> get_distance =
			(latitude_1, longitude_1, latitude_2, longitude_2) ->
				Math.round(2 * EARTH_RADIUS * Math.asin(
					Math.sqrt(
						Math.pow(Math.sin((Math.toRadians(latitude_2) - Math.toRadians(latitude_1)) / 2.0), 2.0)
						+ Math.cos(Math.toRadians(latitude_1)) * Math.cos(Math.toRadians(latitude_2)) *
							Math.pow(Math.sin((Math.toRadians(longitude_2) - Math.toRadians(longitude_1))
								/ 2.0), 2.0)
					)
				)) / 1000.0;
		spark.udf().register("get_distance", get_distance, DataTypes.DoubleType);
		spark_queries.add(df -> df
			.withColumn("distance_km",
				functions.callUDF("get_distance",
					functions.col("latitude_1"), functions.col("longitude_1"),
					functions.col("latitude_2"), functions.col("longitude_2")
				)
			)
			.withColumn("flightdate", functions.to_date(functions.col("firstseen")))
			.groupBy(functions.col("flightdate"))
			.agg(functions.max("distance_km").as("max_distance_km"))
			.select(functions.col("flightdate"), functions.max(functions.col("max_distance_km"))
					.over(ws4).as("max_distance_km_on_date"))
			.orderBy(functions.col("flightdate").asc()).persist()
		);
		for (int i = 0; i < TEST_REPEAT_COUNT; i++) {
			LOGGER.info(String.format("%d iteration", i + 1));
			for(int j = 0; j < spark_queries.size(); j++){
				Function<Dataset<Row>, Dataset<Row>> spark_query = spark_queries.get(j);
				HashMap<String, Long> res = measureQueryExecutionTime(spark, PARQUET_PATH, spark_query);
				executionTime = res.getOrDefault("execution_time", 0L);
				recordsCount = res.getOrDefault("records_count", 0L);
				LOGGER.info(String.format("\t%d-st query: elapsed time %s ms, selected %s records", j + 1,
						withLargeIntegers(executionTime), withLargeIntegers(recordsCount)));
				testTimes[j] += executionTime;
			}
		}
		LOGGER.info("SUMMARY");
		for (int i = 0; i < testTimes.length; i++) {
			testTimes[i] = Math.round((double) testTimes[i] / TEST_REPEAT_COUNT);
			LOGGER.info(String.format("\t %d) %s ms", i + 1, withLargeIntegers(testTimes[i])));
		}
		LOGGER.info("All tests completed successfully");
		spark.stop();
	}
}