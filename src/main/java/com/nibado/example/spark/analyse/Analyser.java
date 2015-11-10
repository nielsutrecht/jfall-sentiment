package com.nibado.example.spark.analyse;

import com.nibado.example.spark.Comment;
import com.nibado.example.spark.Mappers;

import java.io.Serializable;

public class Analyser implements Serializable {
    private WordList list;
    public Analyser(WordList list) {
        this.list = list;
    }

    public AnalysedComment analyse(Comment comment) {
        if(comment.isDeleted()) {
            throw new RuntimeException("Can't analyse deleted comments");
        }

        double score = 0.0;

        for(String word : comment.getBody()) {
            WordList.Score scoreObj = list.get(word);

            if(scoreObj != null) {
                score += scoreObj.getPositive();
                score -= scoreObj.getNegative();
            }
        }

        return AnalysedComment.of(comment, score);
    }
}
