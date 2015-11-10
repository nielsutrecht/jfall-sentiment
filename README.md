# Sentiment Analysis of Social Media Posts with Apache Spark

This repository contains the sample source code and presentation used in the ignite session I have on [JFall 2015](http://www.nljug.org/jfall/2015/).

## Presentation

The presentation (as PDF) can be found [here](/doc/jfall-sentiment.pdf).

## Spark Hello World

A small runnable example of how to do do a word-count analysis is shown in [HelloSparkWorld.java](./src/main/java/com/nibado/example/spark/example/HelloSparkWorld.java).

## Running the analysis

### Downloading the data

The 5GB dataset can be downloader using your favorite torrent client using [this link](https://mega.nz/#!ysBWXRqK!yPXLr25PgJi184pbJU3GtnqUY4wG7YvuPpxJjEmnb9A).

You should end up with a RC_2015-01.bz2 file around 5GB in size.

The application.properties file has the default input set to /tmp/RC_2015-01.bz2. If you downloaded the file to a different
location please change the properties file accordingly.

### Configuration

The application has two config settings that need to be set by you (if their defaults are incorrect), these settings are
contained in application.properties.

The input property should point to RC_2015-01.bz2 you just downloaded. The output property should point to an empty directory. 
The application will create the full directory if possible. 

### Running the Analysis

You can run the analysis by simply starting running the Main class. It should start a spark context and start an analysis
run. You can then connect to http://localhost:4040/ to see the progress. Keep in mind that this process will take quite 
some time, more than one hour on my machine.
 
First it reads all the JSON and parses it into internal comment structures and analyses these. The resulting data is stored
in a temporary object store location. This isn't strictly needed at all but since this part takes by far the most amount of 
time it's done for convenience: running new reduce operations on this dataset takes a lot less time than going through the
entire deserialization again.

The object file is then used to do the count and sentiment reductions which are then written to their corresponding files.

## Links
* Reddit
  * [What is Reddit?](https://en.wikipedia.org/wiki/Reddit)
  * [Original Reddit Post](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment)
  * [Comment data on google bigquery](https://bigquery.cloud.google.com/table/fh-bigquery:reddit_comments.2015_05)
* Apache Spark
  * [What is Apache Spark?](http://spark.apache.org/)
  * [Spark vs Map-Reduce](https://www.quora.com/What-is-the-difference-between-Apache-Spark-and-Apache-Hadoop-Map-Reduce)
  * [Spark Getting Started](http://spark.apache.org/docs/latest/programming-guide.html)
* Sentiment Analysis
  * [What is Sentiment Analysis](https://semantria.com/sentiment-analysis)
  * [SentiNet Word List](http://sentiwordnet.isti.cnr.it/)
  * [Stanford Paper on Sentiment Analysis](http://nlp.stanford.edu/sentiment/)