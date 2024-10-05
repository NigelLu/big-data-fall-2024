package com.nigellu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleIdMapperReducer {

  // region: extra credit
  public static class SimpleIdMapper extends Mapper<Object, Text, IntWritable, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // * the default TextOutputFormat will separate the key-value pairs given by
      // * previous reducer using \t
      String[] prevOutputs = value.toString().split("\t");
      if (prevOutputs.length == 2) {
        String word = prevOutputs[0];
        int frequency = Integer.parseInt(prevOutputs[1]);
        // * reverse the key-value pair to be key: frequency, value: word
        context.write(new IntWritable(frequency), new Text(word));
      }
    }
  }

  public static class SimpleIdReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private int rank = 1; // * global tracker to record what's the ranking id for next word

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // * Iterate through words with the same frequency (key)
      // * hadoop default sorts the key ascending
      // * we'll apply the self-defined comparator written below to force it sort
      // * descending
      for (Text word : values)
        context.write(new IntWritable(rank++), word);
    }

  }

  public static class DescendingIntComparator extends WritableComparator {
    protected DescendingIntComparator() {
      super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes") // * used to suppress the generic type warning in VSCode
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntWritable key1 = (IntWritable) w1;
      IntWritable key2 = (IntWritable) w2;
      // * Reverse the comparison for descending order
      return -1 * key1.compareTo(key2);
    }

  }
  // endregion: extra credit
}
