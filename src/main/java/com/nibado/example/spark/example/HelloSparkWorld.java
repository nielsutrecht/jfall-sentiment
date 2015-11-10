package com.nibado.example.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Example usage of Spark: this loads a file and counts the unique words in this file. It then outputs the top 10 of
 * these counts to the console.
 */
public class HelloSparkWorld {
    public static void main(String... argv) {
        //Create a spark config with a single local instance
        SparkConf config = new SparkConf().setAppName("HelloSparkWorld").setMaster("local[1]");
        //Create a context
        JavaSparkContext sc = new JavaSparkContext(config);

        List<Tuple2<String, Integer>> wordCounts = sc
                .textFile("src/main/resources/loremipsum.txt")  //Load text file
                .map(String::toLowerCase)                       //toLower case
                .map(s -> s.replaceAll("[^a-z ]+", ""))         //Strip anything not alphabetic or space
                .flatMap(s -> Arrays.asList(s.split("\\s+")))   //Split on whitespace
                .filter(s -> s.length() >= 2)                   //Filter words shorter than 2 characters
                .mapToPair(s -> new Tuple2<>(s, 1))             //Map words to (word, count) tuples
                .reduceByKey((a, b) -> a + b)                   //Reduce
                .collect();                                     //Collect into a list

		//Print top 10 results
        wordCounts
                .stream()
                .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                .limit(10)
                .forEach(System.out::println);
    }
}
