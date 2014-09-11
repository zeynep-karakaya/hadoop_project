/**
 * Nach der Anleitung in 
 * https://github.com/hortonworks/hadoop-tutorials/blob/master/Community/T09_Write_And_Run_Your_Own_MapReduce_Java_Program_Poll_Result_Analysis.md
 */
package com.salesaggregation;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalesAggregationApplication extends Configured implements Tool {

	 public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new SalesAggregationApplication(), args);
	    System.exit(res);       
	 }
	 
	public int run(String[] args) throws Exception {
		
		 Path inputFilePathPosH = new Path("hdfs://192.168.178.19:8020/user/hue/csv/posheader.csv");
//		 Path inputFilePathCust = new Path("hdfs://192.168.178.19:8020/user/hue/csv/customer.csv");
		 Path outputFilePath = new Path("hdfs://192.168.178.19:8020/user/hue/SalesAggregationOutput3/");
		 
		 Configuration config = new Configuration();
		    config.set("fs.default.name", "hdfs://192.168.178.19:8020");
		    config.set("mapred.job.tracker", "192.168.178.19:8021");
		    config.set("fs.defaultFS", "hdfs://192.168.178.19:8020/user/hue");
		    config.set("hadoop.job.ugi", "hue");
		    System.out.println("config settings");
		 
//		 if (args.length != 2) {
//	            System.out.println("usage: [input] [output]");
//	            System.exit(-1);
//	        }
	    	System.out.println("hadoophome checked");
	       
	    	        
	        Job job = Job.getInstance(config);
	        System.out.println("job");
	        job.setMapOutputKeyClass(LongWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        System.out.println("key value");
	        job.setMapperClass(SalesAggregationMapper.class);
	        job.setReducerClass(SalesAggregationReducer.class);
	        job.setJarByClass(SalesAggregationApplication.class);
	        System.out.println("mapper reducer application");
//	        job.setInputFormatClass(TextInputFormat.class);
//	        job.setOutputFormatClass(TextOutputFormat.class);
	        System.out.println("input output");
	        job.setNumReduceTasks(1);

	        FileInputFormat.setInputPaths(job, inputFilePathPosH);
	        FileOutputFormat.setOutputPath(job, outputFilePath);
	        System.out.println("path");
	        

	        job.submit();
	        job.waitForCompletion(true);
	        System.out.println("fertig");
	        return 0;
	}

}
