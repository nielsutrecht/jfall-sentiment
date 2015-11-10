package com.nibado.example.spark.analyse;

import com.nibado.example.spark.Comment;
import com.nibado.example.spark.Mappers;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.nibado.example.spark.Mappers.toDayOfWeek;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Created by niels on 2-11-15.
 */
public class AnalysedCommentTest {
    @Test
    public void testTimeStamp() {
        ZonedDateTime dt = ZonedDateTime.of(2015, 11, 9, 12, 0, 0, 0, ZoneId.systemDefault());

        Comment c = new Comment();
        c.setBody(new String[0]);

        c.setTimeStamp(dt.plusDays(0).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("MONDAY"));
        c.setTimeStamp(dt.plusDays(1).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("TUESDAY"));
        c.setTimeStamp(dt.plusDays(2).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("WEDNESDAY"));
        c.setTimeStamp(dt.plusDays(3).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("THURSDAY"));
        c.setTimeStamp(dt.plusDays(4).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("FRIDAY"));
        c.setTimeStamp(dt.plusDays(5).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("SATURDAY"));
        c.setTimeStamp(dt.plusDays(6).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("SUNDAY"));
        c.setTimeStamp(dt.plusDays(7).toInstant().getEpochSecond());
        assertThat(toDayOfWeek(AnalysedComment.of(c, 0.0).getDateTime()), equalTo("MONDAY"));
    }
}