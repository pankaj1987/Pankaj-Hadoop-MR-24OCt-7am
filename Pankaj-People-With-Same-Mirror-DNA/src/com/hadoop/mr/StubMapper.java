package com.hadoop.mr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, Text>{
	
	@Override
	public void map(Object key, Text inputValue, Context context) throws IOException, InterruptedException{

String inputLine = 	inputValue.toString();	
String[] input = inputLine.split("[ \t]+");
		
String DNA = input[1];
char[] DNAcharArr = DNA.toCharArray();
Arrays.sort(DNAcharArr);
System.out.println("map key : "+inputLine);
context.write(new Text(String.valueOf(DNAcharArr)), new Text(inputLine));
	}
}
