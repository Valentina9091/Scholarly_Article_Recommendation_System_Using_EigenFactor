package com.technique3.alef;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This class we compute the teleportation weight wi for each node i is
 * calculated by summing the in and out citations.
 * 
 * We have Zij matrix already. We read that and emit Zij and ZijT in the
 * following format below so that we can sum up Zij with its transpose.
 * 
 * @author Valentina Palghadmal
 *
 */
public class WMatrix extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(WMatrix.class.getName());

	// Main Function definition
	/*
	 * public static void main(String[] args) throws Exception { int res =
	 * ToolRunner.run(new WMatrix(), args); System.exit(res); }
	 */

	// Execute mapper and reducer functions in driver
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Path output = new Path(args[1]+Constant.OUTPUT_DIR_Wi);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "WMatrix");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[1]+Constant.OUTPUT_DIR_ADD));
		TextOutputFormat.setOutputPath(job,output);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Mapper function and in which we add delimiter and file name
	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		// private static final Pattern tabPattern = Pattern.compile("\t");

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().trim();
			String[] lineArr = line.split("\t");
			String ab[] = lineArr[0].split(",");
			//ab[0] = ab[0].replace("(", "");
			//ab[1] = ab[1].replace(")", "");
			context.write(new IntWritable(Integer.parseInt(ab[0])),
					new IntWritable(Integer.parseInt(lineArr[1])));
		}
	}

	// Reducer Function computes word count and term frequency accordingly
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, Text, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> textList,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable a : textList) {
				sum += Integer.parseInt(a.toString());
			}
			context.write(new Text(Constant.TELEPORTATION_WEIGHT+","+key+",1,"+sum),null);

		}
	}

}
