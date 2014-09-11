package com.salesaggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SalesAggregationReducer extends Reducer<Text, Text, Text, Text> {
	LongWritable counter = new LongWritable(0);
	LongWritable zero = new LongWritable(0);

	@Override
	 public void reduce(Text key, Iterable<Text> values, Context output)
	        throws IOException, InterruptedException {
		 
		    System.out.println("reducer");
		    
		    Text text = new Text();
		    for(Text value: values){
		    	String valueString = value.toString();
		    	System.out.println("key: "+key.toString()+" value: "+value );
//	            String[] valueArray = valueString.split("#");
	            text = new Text(text.toString()+" "+value.toString());
	        }
		    
//		    LongWritable v = new LongWritable();
//	        v.set(counter);
//	        output.write(new Text(counter.toString()), new Text(text));
	        output.write(new Text(key.toString()), new Text(text));
	        counter.set(counter.get()+1);;
	    }
	 
}
