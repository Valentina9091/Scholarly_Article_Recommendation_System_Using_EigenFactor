package com.technique3.alef;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.technique3.alef.DotProduct.Map;

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
 * @author Valentina Palghadmal
 *
 */
public class ALEF extends Configured implements Tool {
	// private static float sum = 0f;
	static HashMap<String, Float> map = new HashMap<String, Float>();
	private static final Logger LOG = Logger.getLogger(ALEF.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		// config.set("mapred.jobtracker.taskScheduler.maxRunningTasksPerJob",
		// "1");
		FileSystem hdfs = FileSystem.get(config);
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_H), true);
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_Wi), true);
		Path output1 = new Path(args[1] + Constant.OUTPUT_DIR_ALEF + "_sum");
		// delete existing directory
		if (hdfs.exists(output1)) {
			hdfs.delete(output1, true);
		}
		Job job1 = Job.getInstance(getConf(), "ALEF1");
		job1.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job1, new Path(args[1]
				+ Constant.OUTPUT_DIR_DOT_PRODUCT));
		TextOutputFormat.setOutputPath(job1, output1);

		// job1.getConfiguration().setStrings(Constant.TOTAL_RECORDS,
		// Driver.count + "");
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setNumReduceTasks(1);

		//job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		job1.waitForCompletion(true);

		Path output2 = new Path(args[1] + Constant.OUTPUT_DIR_ALEF);
		// delete existing directory
		if (hdfs.exists(output2)) {
			hdfs.delete(output2, true);
		}
		
		Job job2 = Job.getInstance(getConf(), "ALEF");
		job2.setJarByClass(this.getClass());
		job2.getConfiguration()
		.setStrings("SUM",output1.toString()+"/part-r-00000");

		// MultipleInputs.addInputPath(job2,output1, TextInputFormat.class,
		// Map2.class);
		// MultipleInputs.addInputPath(job2, new Path(args[1]
		// +Constant.OUTPUT_DIR_DOT_PRODUCT), TextInputFormat.class,
		// Map2.class);
		
		TextInputFormat.setInputPaths(job2, new Path(args[1]
				+ Constant.OUTPUT_DIR_DOT_PRODUCT));
		TextOutputFormat.setOutputPath(job2, output2);

		job2.getConfiguration().setStrings(Constant.TOTAL_RECORDS,
				Driver.count + "");
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		//job2.setNumReduceTasks(1);

		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map1 extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().trim();
			String[] lineArr = line.split(",");
			if(lineArr.length>2)
			context.write(new Text("$$SUM$$"),
					new FloatWritable(Float.parseFloat(lineArr[2])));

		}
	}

	public static class Reduce1 extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0f;
			for (FloatWritable f : values) {
				sum += f.get();
			}
			context.write(key, new FloatWritable(sum));
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		static float sum;
		public void setup(Context context) throws IOException,
		InterruptedException {
			String filename=context.getConfiguration().get("SUM","");
			Path inputPath = new Path(filename);// Input file name
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(inputPath)));

			String line;
			// line = br.readLine();
			while ((line = br.readLine()) != null) {
				sum=Float.parseFloat(line.split(Constant.SEPARATOR_TAB)[1]);
			}
			br.close();
		}
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().trim();
			String[] lineArr = line.split(",");

			context.write(new Text(lineArr[1]), new Text(lineArr[0]
					+ Constant.SEPARATOR + lineArr[2]+Constant.SEPARATOR+sum));

		}
	}

	public static class Reduce2 extends
			Reducer<Text, Text, Text, FloatWritable> {
		static int totalRecords;
		//static float summation = 0f;
		public void setup(Context context) throws IOException,
		InterruptedException {
			totalRecords=Driver.count;
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String tempArr[];
			float temp = 0f;
			float sum=0f;
			for (Text val : values) {
				// Log.info("Count  "+totalRecords);
				tempArr = val.toString().split(Constant.SEPARATOR);
				sum=Float.parseFloat(tempArr[2]);
				temp = (totalRecords/sum) * Float.parseFloat(tempArr[1]);
				context.write(new Text(tempArr[0] + "," + key),
						new FloatWritable(temp));
		

			}

		}
	}

}
