package MaxTemp;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;   
        
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    	 
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
        int maxValue = 0;
        for (IntWritable val : values) 
        {
        	maxValue = Math.max(maxValue, val.get());
        }
        context.write(key, new IntWritable(maxValue));
	}
} 


