package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	public void map(Object key, Text inputValue, Context context)throws IOException, InterruptedException{
		String[] inputLine = inputValue.toString().split("[ \t]+");
		context.write(new Text(inputLine[1]), new Text(inputLine[0]));
	}
}
