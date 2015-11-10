package com.nibado.example.spark.analyse;

import com.nibado.example.spark.Comment;
import com.nibado.example.spark.Mappers;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Locale;

import static com.nibado.example.spark.Mappers.toDateString;

public class AnalysedComment implements Serializable {
    public static final long serialVersionUID = 1L;
    public static final double SENTIMENT_CUTOFF = 0.1;
    private String subReddit;
    private String author;
    private long timeStamp;
    private String[] body;
    private int words;
    private double score;
    private double normalizedScore;

    public static AnalysedComment of(Comment comment, double score) {
        AnalysedComment newComment = new AnalysedComment();

        newComment.subReddit = comment.getSubReddit();
        newComment.author = comment.getAuthor();
        newComment.timeStamp = comment.getTimeStamp();
        newComment.words = comment.getBody().length;
        //Uncomment this for the word-count analysis
        //newComment.body = comment.getBody();
        newComment.score = score;
        newComment.normalizedScore = score / (double)newComment.words;

        return newComment;
    }

    public double getScore() {
        return score;
    }

    public double getNormalisedScore() {
        return normalizedScore;
    }

    public boolean isPositive() {
        return normalizedScore > SENTIMENT_CUTOFF;
    }

    public boolean isNegative() {
        return normalizedScore < -SENTIMENT_CUTOFF;
    }

    public boolean isNeutral() {
        return normalizedScore >= -SENTIMENT_CUTOFF && normalizedScore <= SENTIMENT_CUTOFF;
    }

    public String getSubReddit() {
        return subReddit;
    }

    public String getAuthor() {
        return author;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String[] getBody() {
        return body;
    }

    public LocalDateTime getDateTime() {
        return LocalDateTime.ofInstant(new Date(timeStamp * 1000).toInstant(), ZoneId.systemDefault());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s %s: %s %s %.2f", toDateString(timeStamp), subReddit, author, words, getNormalisedScore());
    }
}
