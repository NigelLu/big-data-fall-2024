package com.nigellu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * separate class for arg-parsing
 */
public class CommandLineParser {
  private String inputPath;
  private String outputPath;
  private String extraPath;

  public CommandLineParser(Configuration conf, String[] args) throws Exception {
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();

    for (int i = 0; i < otherArgs.length; i++) {
      switch (otherArgs[i]) {
        case "--input":
          inputPath = otherArgs[++i];
          break;
        case "--output":
          outputPath = otherArgs[++i];
          break;
        case "--extra":
          extraPath = otherArgs[++i];
          break;
        default:
          System.err.println("Usage: --input <input path> --output <output path>");
          throw new IllegalArgumentException("Invalid argument: " + otherArgs[i]);
      }
    }

    // * Validate that both input and output paths are provided
    if (inputPath == null || outputPath == null) {
      System.err.println("Both --input and --output paths are required.");
      throw new IllegalArgumentException("Missing required arguments: --input and --output");
    }
  }

  public String getInputPath() {
    return inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getExtraPath() {
    return extraPath;
  }
}
