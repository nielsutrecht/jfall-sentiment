package com.nibado.example.spark.analyse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A map of words to their negative / positive sentiment scores.
 */
public class WordList implements Serializable {
    private Map<String, Score> words;

    public WordList() {
        words = new HashMap<>();
    }

    public void add(String word, double positive, double negative) {
        add(word, new Score(positive, negative));
    }

    public Score get(String word) {
        return words.get(word);
    }

    public Stream<String> getWords(boolean sort) {
        Stream<String> stream = words.keySet().stream();
        if(sort) {
            stream = stream.sorted();
        }

        return stream;
    }

    public int size() {
        return words.size();
    }

    public void add(String word, Score score) {
        words.put(word, score);
    }

    public static class Score implements Serializable {
        public double positive;
        public double negative;

        public Score(double positive, double negative) {
            this.positive = positive;
            this.negative = negative;
        }

        public double getPositive() {
            return positive;
        }

        public double getNegative() {
            return negative;
        }
    }
}
