package com.hadoop.mr;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StubMapper extends Mapper<Object, Text, Text, Text>{
	@Override
	public void map(Object key, Text inputValue, Context context) throws IOException, InterruptedException{
	String[] eachWord = inputValue.toString().split("[ \t]+");
	  
	  for(int i = 0; i < eachWord.length; i++){
		  if(i < eachWord.length){
			  context.write(new Text(eachWord[i].replaceAll("[^a-zA-Z]+", "").toLowerCase()), new Text(eachWord[i+1].replaceAll("[^a-zA-Z]+", "").toLowerCase()));
		  } 
	  } 
	}
}

