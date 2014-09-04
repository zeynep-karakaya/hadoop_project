package com.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatBaseInputFormat;

public class SalesAggregationMapper extends
		Mapper<Writable, HCatRecord, Text, IntWritable> {

	@Override
	protected void map( Writable key, 
			HCatRecord value, 
			Context context) 
					throws IOException, InterruptedException {
		
		// Get table schema
	     HCatSchema schema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());

	     Integer custid = new Integer(value.getString("custid", schema));
	     Integer transactionid = new Integer(value.getString("transactionid", schema));
	     
	     context.write(new Text(String.valueOf(transactionid)), new IntWritable(custid));
	}
}
