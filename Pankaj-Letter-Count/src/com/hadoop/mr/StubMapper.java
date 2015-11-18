package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, LongWritable> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String words = value.toString().replaceAll("[^a-zA-Z]+", "").toLowerCase();
		  for(int i = 0; i < words.length(); i++){
			  context.write(new Text(String.valueOf(words.charAt(i))), new LongWritable(1));
		  }
	  }
	}
