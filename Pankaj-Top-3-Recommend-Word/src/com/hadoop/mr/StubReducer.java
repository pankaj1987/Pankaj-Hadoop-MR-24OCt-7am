package com.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StubReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text inputKey, Iterable<Text> inputValue, Context context) throws IOException, InterruptedException{
		Map<String, Integer> inputMap = new HashMap<String, Integer>();
		for(Text value : inputValue){
			if(inputMap.containsKey(value)){
				 Integer count = inputMap.get(value);
				 count = count + 1;
				  inputMap.put(value.toString(), count);
			  } else {
				  inputMap.put(value.toString(), 1);
			  }
		}
		
		List<Entry<String, Integer>> inputList = new ArrayList<Entry<String, Integer>>(inputMap.entrySet());
		
		Collections.sort(inputList, new Comparator<Map.Entry<String, Integer>>()
			      {
	          public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
	          {
	              return (o2.getValue()).compareTo( o1.getValue() );
	          }
	      } );

	      int count = 1;  
	      String outputStr = "";
	      for(Map.Entry<String, Integer> entry:inputList){
	    	  if(count <= 3){
	    		  if(outputStr != null && outputStr.length() > 0){
	    			  outputStr = outputStr + ", "+entry.getKey();
	    		  }else {
	    			  outputStr = outputStr + entry.getKey();
	    		  }
	    	  }else {
	    		  break;
	    	  }
	    	  count++;
	      }
		  context.write(inputKey, new Text(outputStr));
	}
}
