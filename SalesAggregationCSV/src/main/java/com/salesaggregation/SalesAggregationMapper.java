package com.salesaggregation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SalesAggregationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	LongWritable counter = new LongWritable(0);
	LongWritable zero = new LongWritable(0);
	boolean flag = false;
	@Override
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
//        System.out.println("mapper");
		String inputString = value.toString();
        String[] values =  inputString.split(",");
        String resultString = "";
        if(counter.equals(zero)){
        	if(values[0].equals("TRANSACTIONID")){
            	resultString = values[0]+","+values[1]+",c";
            	key = new LongWritable(1);
            	flag = true;
            }else if(values[0].equals("\"bdpoc.posheader.transactionid\"")){
            	resultString = values[0]+","+values[2]+",p";
            	key = new LongWritable(2);
            	flag = false;
            } else{
        		//nothing to do
            }
        } else {
        	if(flag){
        		resultString = values[0]+","+values[1]+",c";
        		key = new LongWritable(1);
        	} else {
        		resultString = values[0]+","+values[2]+",p";
        		key = new LongWritable(2);
        	}
        }
              
        
        Text result = new Text(resultString+"#");
        output.write(key, result);
        counter.set(counter.get()+1);
	}
}
