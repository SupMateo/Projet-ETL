package fr.rts.projet.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Example")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Apache Spark Version: " + sc.version());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvFile);

        sc.close();
    }
}