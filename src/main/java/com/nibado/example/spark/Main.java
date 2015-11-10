package com.nibado.example.spark;

import com.nibado.example.spark.analyse.AnalysedComment;
import com.nibado.example.spark.analyse.Analyser;
import com.nibado.example.spark.analyse.WordList;
import com.nibado.example.spark.loader.CondensedLoader;
import com.nibado.example.spark.loader.SentiWordNetLoader;
import com.nibado.example.spark.loader.WordListLoader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private Properties config;
    private SparkFacade facade;

    public Main() {
        config = new Properties();
        try (InputStream ins = Main.class.getResourceAsStream("/application.properties")) {
            config.load(ins);
        }
        catch(IOException e) {
            LOG.error("Could not load application properties.");
            throw new RuntimeException(e);
        }
    }

    public void run() {
        checkConfig();

        facade = new SparkFacade(config.getProperty("appname"), config.getProperty("master"));
        facade.init();


        Analyser analyser = createAnalyser();

        File tempObjects = new File(config.getProperty("output"), "tempObjects");

        /*
         * For convenience the data is deserialized and scored and then stored in a temp directory. This way you don't have
         * to redo this whole phase whenever you want to change or add an analysis. The deserialization is by far
         * the slowest and can take an hour or so (I'm sure it can be made a lot more efficient). The individual reductions
         * only take about 2 minutes each working on an extracted set of around 4GB.s
         */
        if(!tempObjects.exists()) {
            LOG.info("Creating temp object store in {}", tempObjects.getAbsolutePath());
            facade.toObjectFile(config.getProperty("input"), tempObjects.getAbsolutePath(), analyser);
        }
        else {
            LOG.info("Reading objects from store in {}", tempObjects.getAbsolutePath());
        }

        JavaRDD<AnalysedComment> comments = facade.asAnalysedCommentStream(tempObjects.getAbsolutePath());

        facade.sentimentAnalysis(comments, new File(config.getProperty("output")));
    }

    private void checkConfig() {
        File input = new File(config.getProperty("input"));
        if(!input.exists()) {
            configException("Input file %s does not exist", input);
        }
        File output = new File(config.getProperty("output"));

        output.mkdirs();

        if(!output.exists()) {
            configException("Unable to create output dir", output);
        }
        if(output.isFile()) {
            configException("Output directory is a file", output);
        }
        if(output.listFiles().length > 0) {
            LOG.warn("Output directory {} is not empty", output.getAbsolutePath());
        }

        LOG.info("Reading from {}", config.getProperty("input"));
        LOG.info("Writing to   {}", config.getProperty("output"));
    }

    private void configException(String message, File file) {
        String exception = String.format(message, file.getAbsoluteFile());
        LOG.error(exception);
        throw new RuntimeException(exception);
    }



    public Analyser createAnalyser() {
        try {
            return new Analyser(loadWordList());
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private WordList loadWordList() throws IOException {
        final WordList list = new WordList();
        final WordListLoader loader;

        switch(config.getProperty("wordlist.type")) {
            case "condensed":
                loader = new CondensedLoader();
                break;
            case "sentinet":
                loader = new SentiWordNetLoader();
                break;
            default:
                loader = getLoader(config.getProperty("wordlist.type"));
                break;
        }

        LOG.info("WordListLoader type {}", loader.getClass().getName());

        String file = config.getProperty("wordlist.file");

        if(file.startsWith("jar:")) {
            file = file.replace("jar:", "");
            LOG.info("Loading wordlist from jar: {}", file);
            InputStream ins = Main.class.getResourceAsStream(file);
            if(ins == null) {
                throw new RuntimeException("File " + file + " not found in jar.");
            }
            loader.load(list, ins);
        }
        else {
            LOG.info("Loading wordlist from file: {}", file);
            loader.load(list, new File(file));
        }

        LOG.info("Loaded wordlist with {} entries.", list.size());

        return list;
    }

    private static WordListLoader getLoader(String className) {
        try {
            Constructor<?> con = Class.forName(className).getConstructor();
            Object instance = con.newInstance();

            if(!(instance instanceof WordListLoader)) {
                throw new ClassNotFoundException("Class " + className + " does not implement " + WordListLoader.class.getName());
            }

            return (WordListLoader)instance;
        }
        catch(ClassNotFoundException e) {
            throw new RuntimeException("Class " + className + " not found.");
        }
        catch(Exception e) {
            throw new RuntimeException("Class " + className + " does not have a public parameterless constructor.");
        }
    }

    public static void main(String... argv) {
        new Main().run();
    }
}
