package com.hadoop.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class StubDriver extends Configured implements Tool {

	private static final String INTERMEDIATE_OUTPUT_PATH = "/user/t7974/Pankajdir/intermediate-Output.txt";

	 @Override
	 public int run(String[] args) throws Exception {
	  /*
	   * Job 1
	   */
	System.out.println("Inside Run method");
	  Configuration conf = getConf();
	  FileSystem fs = FileSystem.get(conf);
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(StubDriver.class);

	  job.setMapperClass(StubMapper.class);
	  job.setReducerClass(StubReducer.class);

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(LongWritable.class);			
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);

	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_OUTPUT_PATH));

	  job.waitForCompletion(true);

	  /*
	   * Job 2
	   */
	  
	  Job job2 = new Job(conf, "Job 2");
	  job2.setJarByClass(StubDriver.class);

	  job2.setMapperClass(StubMapper2.class);
	  job2.setReducerClass(StubReducer2.class);

	  job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	  return job2.waitForCompletion(true) ? 0 : 1;
	 }

	 /**
	  * Method Name: main Return type: none Purpose:Read the arguments from
	  * command line and run the Job till completion
	  * 
	  */
	 public static void main(String[] args) throws Exception {
	  // TODO Auto-generated method stub
	  if (args.length != 2) {
	   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
	   System.exit(0);
	  }
	  ToolRunner.run(new Configuration(), new StubDriver(), args);
	 }
	
	

}
