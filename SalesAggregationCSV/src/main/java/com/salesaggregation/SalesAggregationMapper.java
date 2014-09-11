package com.salesaggregation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SalesAggregationMapper extends Mapper<Object, Text, Text, Text> {
	LongWritable counter = new LongWritable(0);
	LongWritable zero = new LongWritable(0);
	boolean flag = false;
	@Override
	public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
		
//        System.out.println("mapper");
		String inputString = value.toString();
        String[] values =  inputString.split(",");
        String resultString = "";
        if(counter.equals(zero)){
        	if(values[0].equals("TRANSACTIONID")){
            	resultString = values[1]+",pid";
            	key = new Text(values[0]);
            	flag = true;
            }else if(values[0].equals("\"bdpoc.posheader.transactionid\"")){
            	resultString = values[2].substring(1, values[2].length()-1)+",cid";
            	key = new Text(values[0].substring(1, values[0].length()-1));
            	flag = false;
            } else{
        		//nothing to do
            }
        } else {
        	if(flag){
        		resultString = values[1]+",pid";
        		key = new Text(values[0]);
        	} else {
        		resultString = values[2].substring(1, values[2].length()-1)+",cid";
        		key = new Text(values[0].substring(1, values[0].length()-1));
        	}
        }
              
        
        Text result = new Text(resultString+"#");
        output.write(new Text(key.toString()), result);
        counter.set(counter.get()+1);
	}
}
