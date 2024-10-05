package com.nigellu.bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenMapperReducer {
  /**
   * Takes output from IntSumReducer
   * Extracts the top ten out of the records each Mapper will process
   * In the end, depending on the number of mappers, we may get many top-ten
   * treeMaps
   */
  public static class TopTenMapper extends Mapper<Object, Text, IntWritable, Text> {
    private TreeMap<Integer, String> topWords = new TreeMap<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // * the default TextOutputFormat will separate the key-value pairs given by
      // * previous reducer using \t
      String[] prevOutputs = value.toString().split("\t");
      if (prevOutputs.length == 2) {
        String word = prevOutputs[0];
        int count = Integer.parseInt(prevOutputs[1]);
        topWords.put(count, word);

        // * Keep only top 10 entries
        if (topWords.size() > 10) {
          topWords.remove(topWords.firstKey());
        }
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // * after all the counting, we use cleanup method to finally write the result
      // * of our TreeMap
      for (Map.Entry<Integer, String> entry : topWords.entrySet()) {
        context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
      }
    }
  }

  /**
   * Takes the output of TopTenMapper and combines
   * multiple top-ten treeMaps into one
   */
  public static class TopTenReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private TreeMap<Integer, String> topWords = new TreeMap<>();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text val : values) {
        topWords.put(key.get(), val.toString());
        if (topWords.size() > 10) {
          topWords.remove(topWords.firstKey());
        }
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, String> entry : topWords.descendingMap().entrySet()) {
        context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
      }
    }
  }
}
