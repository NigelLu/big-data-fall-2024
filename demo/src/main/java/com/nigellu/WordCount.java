package com.nigellu;

//java dependencies
import java.io.IOException;
import java.util.StringTokenizer;

// hadoop dependencies
//  maven: org.apache.hadoop:hadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here?)
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Your program main class.
// This class's main `method` will be called by the Hadoop runtime after creating
// an instance of the class
public class WordCount {

  // this is the driver code for your program
  public static void main(String[] args) throws Exception {

    // get a Hadoop configuration instance
    Configuration conf = new Configuration();
    // handle command line arguments
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      // this is Hadoop's Hello World example. Needs two parameters
      // is the input data location or file
      // is where to put the output; must NOT exist
      System.err.println("Usage: wordcount   ");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Juan's WordCount");
    job.setJarByClass(WordCount.class);

    // the input data location
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

    // the input splits.... must match the actual signature
    job.setInputFormatClass(TextInputFormat.class);

    // the mapper: classes and signatures
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // the reducer: classes and signatures
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // where to put the reducer output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    // if we want to increase performance
    // job.setCombinerClass(MyReducer.class);

    // wait for job
    boolean status = job.waitForCompletion(true);

    // we could continue here with other jobs in sequence.....
    // if (status) {
    // job2.....
    // }

    // done
    System.exit(status ? 0 : 1);
  }

  // this class implements the mapper
  public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // this particular mapper input output (K,V) is (Text,IntWritable)
    private final Text word = new Text();
    // we use a constant 1
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // this class implements the reducer
  public static class MyReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {

    // this reducer input must match the mapper's output types: i.e. (Text,
    // IntWritable)

    // the reducer;s output types: (Text, IntWritable)
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}