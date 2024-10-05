package com.nigellu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTenRunner {

    public static void run(String inputPath, String outputPath, Configuration config, Log LOG) throws Exception {
        FileSystem fs = FileSystem.get(config);

        // * intermediate path used for storing tokenCount
        Path tokenPath = new Path("/users/xl3139/xl3139-hw1/tokenCount");
        // * Job 1
        LOG.info(fs.exists(tokenPath));
        if (fs.exists(tokenPath)) {
            fs.delete(tokenPath, true);
            LOG.info("Deleting previous output directory");
            Thread.sleep(1000);
        }
        Job job1 = Job.getInstance(config, "word count");
        job1.setJarByClass(TopTenRunner.class);
        job1.setMapperClass(WordCountMapperReducer.TokenizerMapper.class);
        job1.setCombinerClass(WordCountMapperReducer.IntSumReducer.class);
        job1.setReducerClass(WordCountMapperReducer.IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, tokenPath);
        boolean job1Succeeded = job1.waitForCompletion(true);
        if (!job1Succeeded) {
            System.exit(1);
        }
        LOG.info("Job 1 Succeed");

        // * Job 2
        Path job2OutputPath = new Path(outputPath);
        if (fs.exists(job2OutputPath)) {
            fs.delete(job2OutputPath, true);
            LOG.info("Deleting previous output directory");
            Thread.sleep(1000);
        }
        Job job2 = Job.getInstance(config, "top 10 words");
        job2.setJarByClass(TopTenRunner.class);
        job2.setMapperClass(TopTenMapperReducer.TopTenMapper.class);
        job2.setReducerClass(TopTenMapperReducer.TopTenReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, tokenPath);
        FileOutputFormat.setOutputPath(job2, job2OutputPath);
        boolean job2Succeeded = job2.waitForCompletion(true);
        if (!job2Succeeded) {
            System.exit(1);
        }
        LOG.info("Job 2 Succeed");

        // * clean up the token count intermediate directory
        if (fs.exists(tokenPath)) {
            fs.delete(tokenPath, true); // * Delete the intermediate directory and its contents
            LOG.info("Intermediate directory cleaned up.");
        }
    }
}
