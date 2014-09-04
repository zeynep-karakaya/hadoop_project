/**
 * Nach der Anleitung in 
 * https://github.com/hortonworks/hadoop-tutorials/blob/master/Community/T09_Write_And_Run_Your_Own_MapReduce_Java_Program_Poll_Result_Analysis.md
 */
package com.vivekGanesan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VoteCountApplication extends Configured implements Tool {

	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new VoteCountApplication(), args);
	        System.exit(res);       
	 }
	 
	public int run(String[] args) throws Exception {
		 if (args.length != 2) {
	            System.out.println("usage: [input] [output]");
	            System.exit(-1);
	        }

	        Job job = Job.getInstance(new Configuration());
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);

	        job.setMapperClass(VoteCountMapper.class);
	        job.setReducerClass(VoteCountReducer.class);

	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        job.setJarByClass(VoteCountApplication.class);

	        job.submit();
	        return 0;
	}

}
