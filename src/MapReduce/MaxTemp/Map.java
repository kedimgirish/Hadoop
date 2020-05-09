package MaxTemp;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

 public  class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
 {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        String[] cityTemp=line.split(" ");
        String city = cityTemp[0];
        int temp = Integer.parseInt(cityTemp[1].trim());
        
        context.write(new Text(city), new IntWritable(temp));
    }
 }
