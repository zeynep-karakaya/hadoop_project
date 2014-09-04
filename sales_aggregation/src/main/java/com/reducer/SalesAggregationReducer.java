package com.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;


public class SalesAggregationReducer extends
		Reducer<Text, IntWritable, Writable, HCatRecord> {

	protected void reduce( Text key, 
			Iterable<IntWritable> values, 
			Reducer<Text, IntWritable, Writable, HCatRecord>.Context context)
				throws IOException, InterruptedException {
		
		 Integer count = 0;
		 for(IntWritable v : context.){
			 count++;
			 context.
		 }
		 // define output record schema
		  ArrayList<HCatFieldSchema> columns = new ArrayList<HCatFieldSchema>(2);
		  columns.add(new HCatFieldSchema("custid", HCatFieldSchema.Type.INT, ""));
		  columns.add(new HCatFieldSchema("numtransactions", HCatFieldSchema.Type.INT, ""));
		  HCatSchema schema = new HCatSchema(columns);
		  HCatRecord record = new DefaultHCatRecord(3);
		  
		  record.setInteger("custid", schema, Integer.parseInt(key.toString()));
		  record.set("numtransactions", schema, count);
		  context.write(null, record);
	}
}
