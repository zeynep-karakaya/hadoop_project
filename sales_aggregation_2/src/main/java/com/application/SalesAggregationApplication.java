package com.application;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

import com.mapper.SalesAggregationMapper;
import com.reducer.SalesAggregationReducer;

public class SalesAggregationApplication extends Configured implements Tool {
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
	     Job job = new Job(conf, "SalesAggregation");
	     job.setJarByClass(SalesAggregationApplication.class);
	     job.setMapperClass(SalesAggregationMapper.class);
	     job.setReducerClass(SalesAggregationReducer.class);
	     
	     HCatInputFormat.setInput(job, "bdpoc", "posheader");
	     job.setInputFormatClass(HCatInputFormat.class);
	     
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(DefaultHCatRecord.class);
	     job.setOutputFormatClass(HCatOutputFormat.class);
	     
	     HCatOutputFormat.setOutput(job, OutputJobInfo.create("bdpoc", "transactioncount", null));
	     HCatSchema s = HCatOutputFormat.getTableSchema(job);
	     HCatOutputFormat.setSchema(job, s);
	     return (job.waitForCompletion(true)? 0:1);
	}
	
	public static void main(String[] args) throws Exception{
		  int exitCode = ToolRunner.run(new SalesAggregationApplication(), args);
		  System.exit(exitCode);
	}

}
