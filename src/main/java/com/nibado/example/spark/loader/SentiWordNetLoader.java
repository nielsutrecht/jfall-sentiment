package com.nibado.example.spark.loader;

import com.nibado.example.spark.analyse.WordList;

import java.io.*;

/**
 * Loader for sentiwordnet files.
 */
public class SentiWordNetLoader implements WordListLoader {
    @Override
    public void load(WordList list, InputStream stream) throws IOException {
        try(BufferedReader ins = new BufferedReader(new InputStreamReader(stream))) {
            String line;
            while((line = ins.readLine()) != null) {
                addLine(list, line);
            }
        }
    }

    private void addLine(WordList list, String line) {
        line = line.trim();
        if(line.startsWith("#") || line.equals("")) {
            return;
        }

        String[] parts = line.split("\t");
        double positive = Double.parseDouble(parts[2]);
        double negative = Double.parseDouble(parts[3]);

        if(positive == 0.0 && negative == 0.0) {
            return;
        }
        WordList.Score score = new WordList.Score(positive, negative);

        String[] words = parts[4].split(" ");

        for(String word : words) {
            word = word.substring(0, word.indexOf("#"));
            list.add(word, score);
        }
    }
}
