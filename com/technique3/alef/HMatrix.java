package com.technique3.alef;

import java.io.IOException;

import java.util.logging.Logger;
//import java.util.regex.Pattern;

//import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
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
 * In this class we are forming row stochastic matrix. The matrix Zij is then
 * row normalized so that the sum of each row i equals, we call this Hij
 * 
 * @author Valentina Palghadmal
 *
 */
public class HMatrix extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(HMatrix.class.getName());

	// Execute mapper and reducer functions in driver
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Path output = new Path(args[1] + Constant.OUTPUT_DIR_H);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "HMatrix");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_LINK_GRAPH));
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
			String[] lineArr = line.split("\t");
			context.write(new Text(lineArr[0]), new Text(lineArr[1]));
		}
	}

	// Reducer Function computes word count and term frequency accordingly
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> textList, Context context)
				throws IOException, InterruptedException {
			float temp3 = 0f;
			String temp = " ";
			String temp2[] = new String[2];
			int m = 0;
			int n = 0;
			for (Text ab : textList) {
				temp = (ab.toString());
				temp2 = temp.split(Constant.SEPARATOR);
				m = Integer.parseInt(temp2[0]);
				n = Integer.parseInt(temp2[1]);
				temp3 = m / (float) n;
				// we need to store H transpose
				key = new Text(key.toString().split(",")[1] + ","
						+ key.toString().split(",")[0]);
				context.write(new Text(Constant.ROW_STOCHASTIC_MATRIX + ","
						+ key + "," + temp3), null);

			}

		}
	}

}
