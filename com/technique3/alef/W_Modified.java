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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.technique3.alef.DotProduct.Map;

/**
 * This class used to simplification of W vector for matrix multiplication
 * 
 * @author Valentina Palghadmal
 *
 */
public class W_Modified extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(WMatrix.class.getName());

	// Main Function definition
	/*
	 * public static void main(String[] args) throws Exception { int res =
	 * ToolRunner.run(new WMatrix(), args); System.exit(res); }
	 */

	// Execute mapper and reducer functions in driver
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Path output = new Path(args[1] + Constant.OUTPUT_DIR_W_MOD);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "WMatrix_mod");
		job.setJarByClass(this.getClass());

		MultipleInputs.addInputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_Wi), TextInputFormat.class, Map.class);
		MultipleInputs.addInputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_H), TextInputFormat.class, Map.class);

		TextOutputFormat.setOutputPath(job, output);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Mapper function and in which we add delimiter and file name
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// private static final Pattern tabPattern = Pattern.compile("\t");

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().trim();
			if (line.startsWith(Constant.TELEPORTATION_WEIGHT)) {
				String[] lineArr = line.split(",");
				context.write(new Text(lineArr[1]), lineText);
			} else {
				String[] lineArr = line.split(",");
				context.write(new Text(Integer.parseInt(lineArr[2]) + ""),
						new Text(Integer.parseInt(lineArr[1]) + ""));
			}

		}
	}

	// Reducer Function computes word count and term frequency accordingly
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> textList, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			String temp = "";
			boolean flag = false;
			for (Text a : textList) {
				if (a.toString().startsWith(Constant.TELEPORTATION_WEIGHT)) {
					temp = a.toString();
				} else {
					if (flag) {
						sb.append(a.toString());
						flag = true;
					} else {
						sb.append(Constant.SEPARATOR);
						sb.append(a.toString());

					}
				}
			}
			if (!temp.isEmpty() && sb.length()>0)
				context.write(new Text(temp), new Text(sb.toString()));

		}
	}

}
