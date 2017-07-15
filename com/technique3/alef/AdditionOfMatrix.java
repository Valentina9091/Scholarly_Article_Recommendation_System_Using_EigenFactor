package com.technique3.alef;

import java.io.IOException;

import java.util.logging.Logger;
import java.util.regex.Pattern;

//import org.apache.commons.lang.StringUtils;
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
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class we compute addition of Zij and ZijT .
 * 
 * @author Valentina Palghadmal
 *
 */
public class AdditionOfMatrix extends Configured implements Tool {

	static final Logger LOG = Logger
			.getLogger(AdditionOfMatrix.class.getName());

	// Main Function definition
	/*
	 * public static void main(String[] args) throws Exception { int res =
	 * ToolRunner.run(new AdditionOfMatrix(), args); System.exit(res); }
	 */

	// Execute mapper and reducer functions in driver
	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1] + Constant.OUTPUT_DIR_ADD);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "AdditionOfMatrix");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_LINK_GRAPH));
		TextOutputFormat.setOutputPath(job, output);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Mapper function and in which we add delimiter and file name
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern tabPattern = Pattern.compile("\t");

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().trim();
			String[] lineArr = tabPattern.split(line);
			String key = lineArr[0];

			String ab[] = key.split(",");

			// ab[0] = ab[0].replace("(", "");

			int ok = Integer.parseInt(ab[0]);

			// ab[1] = ab[1].replace(")", "");

			int ok1 = Integer.parseInt(ab[1]);
			String value = lineArr[1];
			String abc[] = value.split("###");

			context.write(new Text(ok + "," + ok1),
					new IntWritable(Integer.parseInt(abc[0])));

			context.write(new Text(ok1 + "," + ok),
					new IntWritable(Integer.parseInt(abc[0])));

		}
	}

	// Reducer Function computes word count and term frequency accordingly
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> textList,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable a : textList) {
				sum += Integer.parseInt(a.toString());
			}
			context.write(key, new IntWritable(sum));
		}
	}

}
