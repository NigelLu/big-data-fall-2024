package com.nigellu.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountMapperReducer {

  /**
   * TokenizerMapper outputs (<token>, <count> -> 1)
   */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    // * constant for outputing token's count when processing
    // * ONE token -> {token:<count> (1)}
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // * replace the consecutive spaces with a single space, also trim all white
      // * space at beginning & end
      String line = value.toString().replaceAll("\\s+", " ").trim();

      // * ignore empty lines
      if (line.isEmpty()) {
        return;
      }

      // * Tokenize words and punctuation
      StringTokenizer itr = new StringTokenizer(line, " .,;?![]'\"", true);
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        if (token.replaceAll("\\s+", "").isEmpty()) {
          continue;
        }
        word.set(token);
        context.write(word, one);
      }
    }
  }

  /**
   * Takes output from TokenizerMapper
   * outputs (<token>, <totalCount>)
   */
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      // * iterate through the values and count them
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      // * output the total count of a particular token
      context.write(key, result);
    }
  }
}
