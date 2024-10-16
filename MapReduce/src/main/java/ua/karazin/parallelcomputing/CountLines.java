package ua.karazin.parallelcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountLines {

    // Mapper class that transforms input lines into key-value pairs.
    public static class LineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);  // Fixed value 1 for each key
        private Text word = new Text();  // Object to store the key word

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input value to a string
            String line = value.toString();
            System.out.println("Processing line: " + line);  // Output the processed line for debugging

            // If the line contains a specific keyword, write it to the context
            if (line.startsWith("A")) {
                word.set("Attribute");
                context.write(word, one);
            } else if (line.startsWith("C")) {
                word.set("Case");
                context.write(word, one);
            } else if (line.startsWith("V")) {
                word.set("Vote");
                context.write(word, one);
            }
        }
    }

    // Reducer class that sums the values for each key.
    public static class LineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Sum up the values for each key
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Write the result to the context
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  // Hadoop configuration
        Job job = Job.getInstance(conf, "Count Lines");  // Create a new job instance
        job.setJarByClass(CountLines.class);  // Set the main class
        job.setMapperClass(LineMapper.class);  // Set the mapper class
        job.setCombinerClass(LineReducer.class);  // Use the reducer as a combiner
        job.setReducerClass(LineReducer.class);  // Set the reducer class
        job.setOutputKeyClass(Text.class);  // The output key will be text
        job.setOutputValueClass(IntWritable.class);  // The output value will be an integer
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Set the input data path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Set the output data path
        System.exit(job.waitForCompletion(true) ? 0 : 1);  // Run the job and exit the program
    }
}
