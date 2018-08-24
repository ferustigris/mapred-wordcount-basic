package com.epam.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * WordCount.
 */
public final class WordCount  {
    /**
     * Constructor.
     */
    private WordCount() {

    }
    /**
     * main.
     * @param args arguments
     * @throws InterruptedException Exception
     * @throws IOException Exception
     * @throws ClassNotFoundException Exception
     */
    public static void main(final String[] args)
            throws InterruptedException, IOException, ClassNotFoundException {

        Job job = Job.getInstance();
        job.setJobName("com.epam.mapreduce.WordCount");
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(WordCountMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("data"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("out"));

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}
