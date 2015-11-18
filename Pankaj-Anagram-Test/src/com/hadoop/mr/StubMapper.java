package com.hadoop.mr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, Text>{
	
	@Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {

	 	  String[] inputLine = value.toString().split("[ \t]+");
		  for(String word:inputLine)
		  {
			 char[] charArray = word.toCharArray();
			 Arrays.sort(charArray);
			 context.write(new Text(String.valueOf(charArray)), new Text(word));
		  }
	  }
	}
