package com.nibado.example.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Locale;

import static com.nibado.example.spark.Mappers.toDateString;

/**
 * Comment POJO that represents the information read from the input.
 */
public class Comment implements Serializable {
    public static final long serialVersionUID = 1L;
    private String subReddit;
    private String author;
    private long timeStamp;
    private String[] body;
    private boolean deleted;

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

    public boolean isDeleted() {
        return deleted;
    }

    public void setSubReddit(String subReddit) {
        this.subReddit = subReddit;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setBody(String[] body) {
        this.body = body;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s %s: %s", toDateString(timeStamp), subReddit, body.length);
    }
}
