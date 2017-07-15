package com.technique3.alef;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Valentina Palghadmal
 *
 */

public class MapEquation_pajek extends Configured implements Tool {
	// Main Function definition

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new MapequationInputPrep(), args);
		System.exit(res);
	}

	// Execute mapper and reducer functions in driver
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Path output = new Path(args[1] + Constant.OUTPUT_DIR_MAP_EQUATION
				+ "_score");
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "Map equation input");
		job.setJarByClass(this.getClass());
		config.set("mapred.jobtracker.taskScheduler.maxRunningTasksPerJob", "1");
		// MultipleInputs.addInputPath(job, new Path(args[1]
		// +Constant.OUTPUT_DIR_ALEF), TextInputFormat.class, Map.class);
		// MultipleInputs.addInputPath(job, new Path(args[1]
		// +Constant.OUTPUT_DIR_LINK_GRAPH), TextInputFormat.class, Map.class);
		FileInputFormat.addInputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_ALEF));
		FileOutputFormat.setOutputPath(job, output);
		job.setNumReduceTasks(1);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Mapper function and in which we add delimiter and file name
	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
		
			String arr[] = lineText.toString().split(Constant.SEPARATOR_TAB);
			if (arr.length == 2) {
				String rowNumber = arr[0].split(Constant.SEPARATOR_COMMA)[0];
				int row=Integer.parseInt(rowNumber);
				context.write(new IntWritable(row ),new Text( " \"Node_" + rowNumber
						+ "\" " + arr[1]));
			}

		}
	}

	public static class Reduce extends Reducer<FloatWritable, Text, Text, NullWritable> {
		NullWritable n = NullWritable.get();
		@Override
		public void reduce(FloatWritable keyText, Iterable<Text> valueList,
				Context context) throws IOException, InterruptedException {
			for(Text val:valueList){
				context.write(new Text(keyText+val.toString()),n);
			}
		}
	}

}
