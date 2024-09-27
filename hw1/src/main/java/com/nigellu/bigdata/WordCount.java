package com.nigellu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;

public class WordCount {
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
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                String word = fields[0];
                int count = Integer.parseInt(fields[1]);
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

    /**
     * separate class for arg-parsing
     */
    public static class CommandLineParser {
        private String inputPath;
        private String outputPath;

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
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        CommandLineParser commandLineParser = new CommandLineParser(conf, args);
        String inputPath = commandLineParser.getInputPath();
        String outputPath = commandLineParser.getOutputPath();

        // * intermediate path used for storing tokenCount
        Path tokenPath = new Path("/users/xl3139/xl3139-hw1/tokenCount");
        // * Job 1
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, tokenPath);
        job1.waitForCompletion(true);

        // * Job 2
        Job job2 = Job.getInstance(conf, "top 10 words");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(TopTenMapper.class);
        job2.setReducerClass(TopTenReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, tokenPath);
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        int job2Succeeded = job2.waitForCompletion(true) ? 0 : 1;
        if (job2Succeeded != 0) {
            System.exit(job2Succeeded);
        }

        // * clean up the token count intermediate directory
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tokenPath)) {
            fs.delete(tokenPath, true); // Delete the intermediate directory and its contents
            System.out.println("Intermediate directory cleaned up.");
        }
    }
}
