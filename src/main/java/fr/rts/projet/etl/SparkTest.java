package fr.rts.projet.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkTest {

    private static final String DB_URL = "jdbc:sqlite:spark_data.db";

    public static void main(String[] args) throws Exception {

        Downloader d = new Downloader("https://archive.ics.uci.edu/dataset/320/student+performance");
        d.downloadAndUnzip();

        SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        File extractedDir = new File("unzipped");
        List<File> csvFiles = findCsvFiles(extractedDir);

        for (File csvFile : csvFiles) {
            String tableName = csvFile.getName().replace(".csv", "").replace("-", "_");
            System.out.println("Processing file: " + csvFile.getAbsolutePath());

            long startSparkTime = System.nanoTime();

            Dataset<Row> df = spark.read().format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ";").load(csvFile.getAbsolutePath());

            df = sanitizeColumnNames(df);

            long endSparkTime = System.nanoTime();
            System.out.println("Spark processing time for " + csvFile.getName() + ": " + (endSparkTime - startSparkTime) / 1_000_000 + " ms");

            long startDbTime = System.nanoTime();

            saveDataFrameToSQLite(df, tableName);

            // Generate and save statistics
            Dataset<Row> statsDf = computeStatistics(df, tableName);
            saveDataFrameToSQLite(statsDf, tableName + "_stats");

            long endDbTime = System.nanoTime();
            System.out.println("Database save time for " + csvFile.getName() + ": " + (endDbTime - startDbTime) / 1_000_000 + " ms");

            // Read back and verify
            Dataset<Row> dfFromDb = readFromDatabase(spark, tableName);
            System.out.println("Data read from database - count: " + dfFromDb.count());
            System.out.println("Database Schema:");
            dfFromDb.printSchema();
        }
        sc.close();

    }

    private static Dataset<Row> computeStatistics(Dataset<Row> df, String tableName) {
        List<String> numericCols = Arrays.stream(df.schema().fields())
                .filter(field -> field.dataType() instanceof NumericType)
                .map(StructField::name)
                .collect(Collectors.toList());

        List<String> textCols = Arrays.stream(df.schema().fields())
                .filter(field -> field.dataType() instanceof StringType)
                .map(StructField::name)
                .collect(Collectors.toList());

        Dataset<Row> numericStats = df.selectExpr(numericCols.stream()
                .flatMap(col -> Arrays.stream(new String[]{
                        "AVG(" + col + ") AS " + col + "_mean",
                        "STDDEV(" + col + ") AS " + col + "_stddev"
                }))
                .toArray(String[]::new));

        Dataset<Row> textStats = df.selectExpr(textCols.stream()
                .map(col -> "FIRST(" + col + ") AS " + col + "_mode")
                .toArray(String[]::new));

        return numericStats.crossJoin(textStats)
                .withColumn("table_name", functions.lit(tableName));
    }

    private static List<File> findCsvFiles(File directory) {
        return Arrays.stream(Objects.requireNonNull(directory.listFiles()))
                .filter(file -> file.isDirectory() || file.getName().endsWith(".csv"))
                .flatMap(file -> file.isDirectory() ? findCsvFiles(file).stream() : Stream.of(file))
                .collect(Collectors.toList());
    }

    private static void saveDataFrameToSQLite(Dataset<Row> df, String tableName) {
        try {
            // Create database and table
            createDatabase(df, tableName);

            // Save data
            Properties properties = new Properties();
            properties.setProperty("driver", "org.sqlite.JDBC");

            df.write()
                    .mode(SaveMode.Overwrite)
                    .jdbc(DB_URL, tableName, properties);

            System.out.println("Data successfully saved to SQLite database");
        } catch (Exception e) {
            System.err.println("Error saving to database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void createDatabase(Dataset<Row> df, String tableName) {
        try {
            // Register JDBC driver
            Class.forName("org.sqlite.JDBC");

            // Generate CREATE TABLE SQL based on DataFrame schema
            String createTableSQL = generateCreateTableSQL(df.schema(), tableName);

            // Create database and table
            try (Connection conn = DriverManager.getConnection(DB_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                stmt.executeUpdate(createTableSQL);
                System.out.println("Database and table created successfully");
                System.out.println("Created table with SQL: " + createTableSQL);
            }
        } catch (Exception e) {
            System.err.println("Error creating database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static String generateCreateTableSQL(StructType schema, String tableName) {
        List<String> columnDefinitions = Arrays.stream(schema.fields())
                .map(field -> "\"" + field.name() + "\" " + getSQLiteType(field.dataType()))
                .collect(Collectors.toList());

        return "CREATE TABLE \"" + tableName + "\" (" +
                String.join(", ", columnDefinitions) +
                ")";
    }

    private static Dataset<Row> sanitizeColumnNames(Dataset<Row> df) {
        for (StructField field : df.schema().fields()) {
            String sanitized = field.name().replaceAll("[^a-zA-Z0-9_]", "_");
            df = df.withColumnRenamed(field.name(), sanitized);
        }
        return df;
    }

    private static String getSQLiteType(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return "INTEGER";
        } else if (dataType instanceof LongType) {
            return "INTEGER";
        } else if (dataType instanceof DoubleType) {
            return "REAL";
        } else if (dataType instanceof FloatType) {
            return "REAL";
        } else if (dataType instanceof StringType) {
            return "TEXT";
        } else if (dataType instanceof BooleanType) {
            return "INTEGER";
        } else if (dataType instanceof TimestampType) {
            return "TEXT";
        } else if (dataType instanceof DateType) {
            return "TEXT";
        } else {
            return "TEXT";
        }
    }

    private static Dataset<Row> readFromDatabase(SparkSession spark, String tableName) {
        Properties properties = new Properties();
        properties.setProperty("driver", "org.sqlite.JDBC");

        return spark.read()
                .jdbc(DB_URL, tableName, properties);
    }
}
