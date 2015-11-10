package com.nibado.example.spark;

import com.nibado.example.spark.analyse.AnalysedComment;
import com.nibado.example.spark.analyse.Analyser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.List;

import static java.util.Arrays.asList;

public class SparkFacade {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFacade.class);

    private SparkConf config;
    private JavaSparkContext sc;

    public SparkFacade(String appName, String master) {
        config = new SparkConf().setAppName(appName).setMaster(master);
    }

    public void init() {
        sc = new JavaSparkContext(config);
    }

    private JavaRDD<Comment> asCommentStream(String file, boolean filterDeleted) {
        return sc.textFile(file)
                .map(Mappers::toComment)
                .filter(c -> !filterDeleted || !c.isDeleted());
    }

    public JavaRDD<AnalysedComment> asAnalysedCommentStream(String file) {
        return sc.objectFile(file);
    }

    public void sentimentAnalysis(JavaRDD<AnalysedComment> comments, File output) {
        List<Tuple2<String, Integer>> results;

        results = comments
                .filter(AnalysedComment::isPositive)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "subPositive.csv"));

        results = comments
                .filter(AnalysedComment::isNegative)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "subNegative.csv"));

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "subTotal.csv"));

        results = comments
                .filter(AnalysedComment::isNegative)
                .mapToPair(c -> new Tuple2<>(Mappers.toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "dayNegative.csv"));

        results = comments
                .filter(AnalysedComment::isPositive)
                .mapToPair(c -> new Tuple2<>(Mappers.toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "dayPositive.csv"));

        results = comments
                .mapToPair(c -> new Tuple2<>(Mappers.toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "dayTotal.csv"));

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getAuthor(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "authorTotal.csv"));

        /*
        results = comments
                .flatMap(c -> asList(c.getBody()))
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, new File(output, "wordTotal.csv"));
        */
    }

    public static void writeTuples(List<Tuple2<String, Integer>> list, File file) {
        LOG.info("Writing {} tuples to {}", list.size(), file.getAbsolutePath());
        final PrintWriter outs;
        try {
            outs = new PrintWriter(new FileWriter(file));
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        list.stream()
                .map(t -> t._1() + ", " + t._2())
                .forEach(outs::println);

        outs.close();
    }

    public void toObjectFile(String inFile, String outFile, Analyser analyser) {
        asCommentStream(inFile, true)
            .map(analyser::analyse)
            .saveAsObjectFile(outFile);
    }

}
