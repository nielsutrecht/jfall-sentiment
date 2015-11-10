package com.nibado.example.spark.analyse;

import com.nibado.example.spark.Comment;
import com.nibado.example.spark.Mappers;
import com.nibado.example.spark.loader.CondensedLoader;
import com.nibado.example.spark.loader.WordListLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static com.nibado.example.spark.Mappers.toWords;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;

public class AnalyserTest {
    private WordList list;
    private Analyser analyser;

    @Before
    public void setup() throws Exception {
        if (list == null) {
            list = loadWordList();
        }

        analyser = new Analyser(list);
    }

    @Test
    public void testPositive() {
        AnalysedComment analysedComment = analyser.analyse(comment("I absolutely see the cachet of this pattern"));

        assertThat(analysedComment.getNormalisedScore(), greaterThan(0.0));
    }

    @Test
    public void testNegative() {
        AnalysedComment analysedComment = analyser.analyse(comment("Such an abhorrent sense of betrayal"));

        assertThat(analysedComment.getNormalisedScore(), lessThan(1.0));
    }

    private static WordList loadWordList() throws IOException {
        final WordList list = new WordList();
        final WordListLoader loader = new CondensedLoader();

        String file = "/sentiwordnet.txt";

        InputStream ins = AnalyserTest.class.getResourceAsStream(file);
        loader.load(list, ins);

        return list;
    }

    private Comment comment(String body) {
        Comment comment = new Comment();
        comment.setBody(toWords(body));

        return comment;
    }
}