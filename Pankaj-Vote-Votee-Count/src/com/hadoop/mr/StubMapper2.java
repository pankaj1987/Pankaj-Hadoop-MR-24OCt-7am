package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper2 extends Mapper<Object, Text, Text, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  String[] inputWord = value.toString().split("[ \t]+");
	  context.write(new Text(inputWord[0]), new LongWritable(new Long(inputWord[1])));
  }
}