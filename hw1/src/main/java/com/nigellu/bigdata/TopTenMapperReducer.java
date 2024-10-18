package com.nigellu.bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;

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
    // * use a list here as the value to accomodate for
    // * tokens that have the same frequency
    private TreeMap<Integer, List<String>> topWords = new TreeMap<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // * the default TextOutputFormat will separate the key-value pairs given by
      // * previous reducer using \t
      String[] prevOutputs = value.toString().split("\t");
      if (prevOutputs.length == 2) {
        String word = prevOutputs[0];
        int count = Integer.parseInt(prevOutputs[1]);
        topWords.computeIfAbsent(count, k -> new ArrayList<>()).add(word);

        // * Keep only top 10 entries
        if (topWords.size() > 10) {
          topWords.remove(topWords.firstKey());
        }
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // * after all the counting, we use cleanup method to finally write the result
      // * of our TreeMap
      for (Map.Entry<Integer, List<String>> entry : topWords.entrySet()) {
        for (String word : entry.getValue()) {
          context.write(new IntWritable(entry.getKey()), new Text(word));
        }
      }
    }
  }

  /**
   * Takes the output of TopTenMapper and combines
   * multiple top-ten treeMaps into one
   */
  public static class TopTenReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private TreeMap<Integer, List<String>> topWords = new TreeMap<>();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text val : values) {
        topWords.computeIfAbsent(key.get(), k -> new ArrayList<>()).add(val.toString());

        if (topWords.size() > 10) {
          topWords.remove(topWords.firstKey());
        }
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // * track the number of tokens written to account for
      // * multiple tokens having the same frequency
      // * this limits our output tokens to 10 unless
      // * the 10th token have the same frequency as the 11th, 12th, or even more
      int totalTokens = 0;
      int lastFrequency = -1;
      for (Map.Entry<Integer, List<String>> entry : topWords.descendingMap().entrySet()) {
        for (String word : entry.getValue()) {
          if (totalTokens >= 10 && lastFrequency != entry.getKey())
            break;

          context.write(new Text(word), new IntWritable(entry.getKey()));
          totalTokens++;
          lastFrequency = entry.getKey();
        }
      }
    }
  }
}
