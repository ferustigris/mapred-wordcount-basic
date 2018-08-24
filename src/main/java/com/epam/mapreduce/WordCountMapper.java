package com.epam.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper.
 */
public class WordCountMapper extends
        Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * map.
     * @param key - key
     * @param value value
     * @param context context
     * @throws IOException Exception
     * @throws InterruptedException Exception
     */
    protected final void map(final LongWritable key,
                       final Text value,
                       final Mapper<LongWritable,
                               Text,
                               Text,
                               LongWritable>.Context context)
            throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            context.write(new Text(s), new LongWritable(1));
        }
    }
}
