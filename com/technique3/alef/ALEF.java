package com.technique3.alef;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This class computes the article level eigen factor. The ALEF scores are
 * calculated by multiplying wi by Hij and normalizing the scores by the number
 * of papers, n, in the corpus
 * 
 * HijT – n*n matrix Wi – teleportation weight vector
 * 
 * We will multiply HijT with Wi using a MR job and write it to an output file.
 * One more MR job to compute the scalar value in the denominator. And finally,
 * we compute ALEF, from the output of the two MR jobs.
 * 
 * 
 * @author Group_10
 *
 */
public class ALEF extends Configured implements Tool {
	private static float sum = 0f;

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		FileSystem hdfs = FileSystem.get(config);
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_H), true);
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_Wi), true);
		Path output = new Path(args[1] + Constant.OUTPUT_DIR_ALEF);
		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		Job job = Job.getInstance(getConf(), "ALEF");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_DOT_PRODUCT));
		TextOutputFormat.setOutputPath(job, output);

		job.getConfiguration().setStrings(Constant.TOTAL_RECORDS,
				Driver.count + "");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().trim();
			String[] lineArr = line.split(",");
			sum += Float.parseFloat(lineArr[2]);
			context.write(new Text(lineArr[1]), new Text(lineArr[0]
					+ Constant.SEPARATOR + lineArr[2]));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, FloatWritable> {
		int totalRecords;

		public void setup(Context context) throws IOException,
				InterruptedException {
			totalRecords = context.getConfiguration().getInt(
					Constant.TOTAL_RECORDS, 1);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String tempArr[];
			float temp = 0f;
			for (Text val : values) {
				tempArr = val.toString().split(Constant.SEPARATOR);
				temp = totalRecords * (Float.parseFloat(tempArr[1]) / sum);
				context.write(new Text(tempArr[0] + "," + key),
						new FloatWritable(temp));

			}

		}
	}

}
