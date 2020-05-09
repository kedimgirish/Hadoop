package AlphabetCount;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public  class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
    	System.out.println("This is a Wrod count problem");
        String line = value.toString();
        String[] splits = line.split("");
        for(String s : splits)
        {
        	context.write(new Text(s), one);
        }
    }
}
