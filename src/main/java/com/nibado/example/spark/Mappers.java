package com.nibado.example.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * For convenience the '.map' operations are contained in this utility class.
 */
public class Mappers {
    private static final ObjectMapper TO_JSON = new ObjectMapper();
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Comment toComment(String s) {
        final JsonNode node;
        try {
            node = TO_JSON.readTree(s);
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        Comment comment = new Comment();
        comment.setTimeStamp(Long.parseLong(node.get("created_utc").asText()));
        comment.setSubReddit(node.get("subreddit").asText().toLowerCase());
        comment.setAuthor(node.get("author").asText().toLowerCase());
        comment.setDeleted("[deleted]".equals(comment.getAuthor()));

        if(comment.isDeleted()) {
            comment.setBody(null);
            comment.setAuthor(null);
        }
        else {
            comment.setBody(toWords(node.get("body").asText()));
        }

        return comment;
    }

    public static String toDayOfWeek(LocalDateTime dt) {
        String day = dt.getDayOfWeek().toString();

        return day;
    }

    public static String toDateString(long timeStamp) {
        return DATE_FORMAT.format(new Date(timeStamp * 1000));
    }

    public static String[] toWords(String s) {
        s = s.substring(s.lastIndexOf("|") + 1);
        s = s.toLowerCase();
        s = s.replaceAll("[^a-z ]+", " ");

        return s.split("\\s+");
    }
}
