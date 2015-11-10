package com.nibado.example.spark.util;

import com.nibado.example.spark.analyse.WordList;
import com.nibado.example.spark.loader.SentiWordNetLoader;
import com.nibado.example.spark.loader.WordListLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;

/**
 * Utility class that creates a more condensed form of the sentinet word list.
 */
public class CondensedWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CondensedWriter.class);

    public static void write(WordList list, File file) throws IOException {
        try(PrintWriter outs = new PrintWriter(new FileWriter(file))) {
            list.getWords(true).forEach(w -> {
                WordList.Score s = list.get(w);
                outs.printf(Locale.ROOT, "%s,%s,%s\n", w, s.getPositive(), s.getNegative());
            });
        }
    }

    public static void convert(File in, File out, WordListLoader loader) throws IOException  {
        WordList list = new WordList();
        LOG.info("Loading");
        loader.load(list, in);
        LOG.info("Writing");
        write(list, out);
        LOG.info("Done");
    }

    public static void main(String... argv) throws Exception {
        convert(new File("/tmp/SentiWordNet_3.0.0_20130122.txt"), new File("src/main/resources/sentiwordnet.txt"), new SentiWordNetLoader());
    }
}
