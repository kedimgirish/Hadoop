package MaxYearTemp;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import MaxTemp.Map;

public class EntryClass 
{
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(EntryClass.class);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		

		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		job.setMapperClass(Mapper_MaxYearTemp.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    FileSystem fs = FileSystem.get(conf);
	    fs.delete(new Path(args[1]));
	    
	     
	    job.waitForCompletion(true);
	}

}
