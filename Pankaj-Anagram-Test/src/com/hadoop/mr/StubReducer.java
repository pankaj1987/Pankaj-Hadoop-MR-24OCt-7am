package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException{	
		String outputString = "";
		values.toString().split(",");
		for(Text value : values){
			if(outputString != null && outputString.length() > 0){
				outputString = outputString+", "+value.toString();
			}else {
				outputString = outputString+value.toString();
			}
		}
		context.write(new Text(""), new Text(outputString));
	}
}
