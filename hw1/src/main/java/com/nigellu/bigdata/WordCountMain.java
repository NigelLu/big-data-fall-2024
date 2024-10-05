package com.nigellu.bigdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class WordCountMain {
  private static final Log LOG = LogFactory.getLog(WordCountMain.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");

    CommandLineParser commandLineParser = new CommandLineParser(conf, args);
    String inputPath = commandLineParser.getInputPath();
    String outputPath = commandLineParser.getOutputPath();
    String extraPath = commandLineParser.getExtraPath();

    TopTenRunner.run(inputPath, outputPath, conf, LOG);
    SimpleIdRunner.run(inputPath, extraPath, conf, LOG);

  }
}
