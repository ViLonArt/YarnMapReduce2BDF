package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreesMaxHeightMapper extends Mapper<Object, Text, Text, FloatWritable> {
    public int curr_line = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (curr_line != 0) {
            try {
                Float height = Float.parseFloat(value.toString().split(";")[6]);
                context.write(new Text(value.toString().split(";")[3]), new FloatWritable(height));
            } catch (NumberFormatException ex) {
                // Gets rid of non float value
            }
        } curr_line++;
    }
}