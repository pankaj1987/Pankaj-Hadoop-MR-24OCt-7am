package com.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, Text, Text, Text>{
	
	//Take the key and value from mapper,
	/*1:Iterate input and convert it into string with comma separated
	2:Iterating each input, take dna of each user/dna set and compare with the dna of remaining user/dna set, take user Id of those which fulfill thr requirement
	3:Add User id to the list
	4:Sort the list , iterate the list , add each user to a string with separated and add to the Context object with user as value and key as empty*/
	public void reduce(Text inputKey, Iterable<Text> inputValue, Context context) throws IOException, InterruptedException{
		List<String> output = new ArrayList<String>();
		String inputLine = "";
		System.out.println(""+inputValue);
		for(Text value : inputValue){
			if(inputLine != null && inputLine.length() > 0){
				inputLine = inputLine+","+value.toString();
			}else {
				inputLine = inputLine+value.toString();
			}
		}
			String[] str = inputLine.split(",");
			List ignoreIndex = new ArrayList();
			
			for(int i=0; i< str.length; i++){
				if(!ignoreIndex.contains(i)){
					String key = str[i];
					String[] input = key.split("[ \t]+");
					 String dna = input[1];
					 output.add(input[0]);
					 for(int j=i+1; j< str.length; j++){				
						String key1 = str[j];
						String[] input1 = key1.split("[ \t]+");
						if(dna.equalsIgnoreCase(input1[1])){
							ignoreIndex.add(j);
							output.add(input1[0]);
						}else {
						String dna1 = new StringBuilder( input1[1]).reverse().toString();
						
						if(dna.equalsIgnoreCase(dna1)){
							ignoreIndex.add(j);
							output.add(input1[0]);
						}
						}
						
					}
				}
				
			}
		//}
			
		Collections.sort(output);
		String userList = "";
		for(String userId: output){
			
			if(userList != null && userList.length() > 0){
				userList = userList+", "+userId;
			}else{
				userList = userList+userId;
			}	
		}
		context.write(new Text(""), new Text(userList));

		
	}
}
