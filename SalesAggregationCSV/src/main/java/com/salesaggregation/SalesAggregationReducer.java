package com.salesaggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SalesAggregationReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	 public void reduce(LongWritable key, Iterable<Text> values, Context output)
	        throws IOException, InterruptedException {
		 
		    System.out.println("reducer");
		    int counter = 0;
		    Text text = new Text();
		    for(Text value: values){
	            counter++;
	            text = value;
	        }
		    
//		    LongWritable v = new LongWritable();
//	        v.set(counter);
	        output.write(new Text(key.toString()), new Text(text+" "+counter));
	    }
	 
}
