package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  String[] inputWord = value.toString().split("[ \t]+");
	  context.write(new Text(inputWord[0]), new Text(inputWord[1]));
  }
}