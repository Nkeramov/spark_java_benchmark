# Spark Java benchmark

This project provides an example of working with Apache Spark Java API. I used Spark ver. 4.0.0

The project was part of a performance comparison challenge of Spark APIs for Java, Python and Scala.
On some dataset, the same set of queries was executed, and then the time of different APIs was compared.

## Dataset

The source dataset is the OpenSky Network 2020 air traffic dataset illustrating the evolution of air traffic during 
the COVID-19 pandemic. The set includes all flights that have been seen by more than 2,500 network members since January 1, 2019.

Source: https://zenodo.org/records/7923702

Martin Strohmeier, Xavier Olive, Jannis Lübbe, Matthias Schäfer, and Vincent Lenders "Crowdsourced air traffic data from the OpenSky Network 2019–2020" Earth System Science Data 13(2), 2021 https://doi.org/10.5194/essd-13-357-2021

For loading the dataset run the command:
```
wget -O- https://zenodo.org/records/7923702 | grep -oP 'https://zenodo.org/records/7923702/files/flightlist_\d+_\d+\.csv\.gz' | uniq | xargs wget
```
or so with [aria2c](https://aria2.github.io/manual/en/html/index.html)
```
wget -O- https://zenodo.org/records/7923702 | grep -oP 'https://zenodo.org/records/7923702/files/flightlist_\d+_\d+\.csv\.gz' | uniq | aria2c -x3 -j6 --max-tries=0 --summary-interval=0 -i -
```

The download will take a few minutes if you have a good internet connection. 30 files will be downloaded with a total size of about 4.5 GB (archives with CSV-files).

## Prerequisites
* Apache Spark 4.0.0
* Java 21 JDK
* Maven


## Building

For build run the command:
```
clean install -U -X
```

## Data preprocessing

First you need to increase the amount of data and convert the data set to the parquet format. This is what the preprocessParquetDataset method does. The increase in the volume of data is performed by duplicating the original data with a shift in the values of time stamps by 3 and 6 years ago. 

The readParquetDataset method allows you to read the dataset, select the required columns, filter empty values, and perform type conversion for some columns.

After reading the dataset has the following schema:
```
root
|-- origin: string (nullable = true)
|-- destination: string (nullable = true)
|-- firstseen: timestamp (nullable = true)
|-- lastseen: timestamp (nullable = true)
|-- latitude_1: double (nullable = true)
|-- longitude_1: double (nullable = true)
|-- latitude_2: double (nullable = true)
|-- longitude_2: double (nullable = true)
```
The dataset contains 215,704,476 records (elapsed time about 1,5-2 minutes).

## Queries

Used 5 queries:
1. Reading and filtering. Obtaining data on all flights from Moscow airports for the summer of 2020.
2. Reading and aggregation. Obtaining the average flight duration grouped by the airport of departure.
3. Reading, aggregation and filtering. Grouping data by airports of departure and arrival to obtain the average flight duration, a sample of flights where the average flight duration is higher than the average duration of all flights in the dataset.
4. Reading and adding a calculated column using a window function. Calculation of distance between airports of departure and arrival. Grouping by date of flight and calculating the maximum distance for the last 30 days from the date of flight.
5. The same as the 4th request. Only using a UDF.

The measureQueryTime method executes reads the dataset and executes the query. Method returns the query execution time and the number of records.

## Test methodology

When executing each query, the data set was first read, and then the records were fetched.

For each request, 10 repetitions are performed, then the request execution time is averaged. The spark cache is cleared before each query execution. All results are logged to a file.

## Results

Of course, the query execution time will depend primarily on the resources of your machine. 
On my machine I got the following results (8 worker threads, 2gb memory per executor):

| query index | average time, ms | selected records  | 
|:-----------:|:----------------:|:-----------------:|
|      1      |      7 sec       |      12,529       |
|      2      |      16 sec      |     2,392,359     |
|      3      |      58 sec      |     1,373,106     |
|      4      |      26 sec      |       3,653       |
|      5      |      26 sec      |       3,653       |

When analyzing the results, it should be taken into account that the execution time includes the time to read the dataset from the disk.