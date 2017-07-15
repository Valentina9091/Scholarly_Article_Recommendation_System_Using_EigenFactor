package com.technique3.alef;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

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
import org.mortbay.log.Log;

import com.technique3.alef.DotProduct.Map;


/**
 * @author Valentina Palghadmal
 *
 */

public class Search extends Configured implements Tool {

	static String searchTitle;
	static String searchIndex;
	static String clusterNumber;
	static String cno;

	// static int clusterNo;

	public static void main(String[] args) throws Exception {

		searchTitle=searchIndex(args);
		System.out.println(searchIndex);
		getClusterNumber(args);
		System.out.println("Cluster number: " + clusterNumber);
		// clusterNo = Integer.parseInt(clusterNumber.split("_")[1]);
		String s[]=new String[args.length+2];
		for(int i=0;i<args.length;i++){
			s[i]=args[i];
		}
		s[3]=searchIndex;
		s[4]=clusterNumber;

		int res = ToolRunner.run(new Search(), s);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Path output = new Path(args[2] + Constant.OUTPUT_DIR_MAP_EQUATION
				+ "_search_1");
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "Search documents");
		job.setJarByClass(this.getClass());

		// input .tree file
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, output);
		job.getConfiguration().setStrings("SearchIndex",args[3]);
		job.getConfiguration().setStrings("ClusterName",args[4]);
		job.setMapperClass(Map.class);
		// job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		Path output2 = new Path(args[2] + Constant.OUTPUT_DIR_MAP_EQUATION
				+ "_search_inter");
		hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output2)) {
			hdfs.delete(output2, true);
		}

		Job job2 = Job.getInstance(config, "Search documents2");
		job2.setJarByClass(this.getClass());

		MultipleInputs.addInputPath(job2, new Path(args[0]),
				TextInputFormat.class, Map2.class);
		MultipleInputs.addInputPath(job2, output, TextInputFormat.class,
				Map2.class);
		// FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, output2);
		// job2.getConfiguration().setStrings("filename",output.toString()+"/part-r-00000"
		// );
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
		
		Path output3 = new Path(args[2] + Constant.OUTPUT_DIR_MAP_EQUATION
				+ "_search_final");
		hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output3)) {
			hdfs.delete(output3, true);
		}

		Job job3 = Job.getInstance(config, "Search documents2");
		job3.setJarByClass(this.getClass());

		
		FileInputFormat.addInputPath(job3, output2);
		FileOutputFormat.setOutputPath(job3, output3);
		job3.getConfiguration().setStrings("title",searchTitle);
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);
		job3.setNumReduceTasks(1);

		job3.setMapOutputKeyClass(FloatWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		return job3.waitForCompletion(true)?0:1;
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line[] = lineText.toString().split(Constant.SEPARATOR_TAB);
			if (lineText.toString().contains("$$value$$")) {

				context.write(new Text(line[0]), new Text(line[1] + line[2]));
			} else {
				if(line.length>1)
				context.write(new Text(line[0]), new Text(line[1]));
			}

		}
	}

	public static class Reduce2 extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text keyText, Iterable<Text> valueList,
				Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			float tempValue = 0f;
			for (Text val : valueList) {
				if (val.toString().contains("$$value$$")) {
					System.out.println(val);
					tempValue = Float.parseFloat(val.toString().replace("$$value$$",""));
				} else {
					sb.append(val.toString());
				}

			}
			if (tempValue > 0) {
				context.write(new Text(sb.toString()), new Text(tempValue + ""));
			}

		}
	}

	public static class Map3 extends
			Mapper<LongWritable, Text, FloatWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line[] = lineText.toString().split(Constant.SEPARATOR_TAB);
			context.write(new FloatWritable(Float.parseFloat(line[1]) * -1),
					new Text(line[0]));

		}
	}

	public static class Reduce3 extends Reducer<FloatWritable, Text, Text, Text> {

		// static int counter=1;
		NullWritable n = NullWritable.get();
		public void setup(Context context) throws IOException,
		InterruptedException {
			context.write(new Text(context.getConfiguration().get("title")),new Text());
			
		}
		@Override
		public void reduce(FloatWritable keyText, Iterable<Text> valueList,
				Context context) throws IOException, InterruptedException {
			for (Text val : valueList) {
				context.write(val,new Text());
			}
		}
	}

	// Mapper function and in which we add delimiter and file name
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		static String searchIndex,clusterNumber;
		public void setup(Context context) throws IOException,
		InterruptedException {
			searchIndex = context.getConfiguration().get("SearchIndex", "");
			clusterNumber = context.getConfiguration().get("ClusterName","");
}

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// NullWritable n = NullWritable.get();
			Log.info(clusterNumber+"----------"+searchIndex);

			if ( lineText!=null && !lineText.toString().isEmpty() &&(lineText.toString().startsWith(clusterNumber.trim() + ":"))
					&& !(lineText.toString().contains("Node_" + searchIndex))) {
				// System.out.println(lineText);
				String lineArr[] = lineText.toString().split(" ");
				if (lineArr.length > 3) {
					String key = lineArr[2].split("_")[1].replace("\"", "");
					// String key = Float.parseFloat(lineArr[3]);
					context.write(new Text(key), new Text(lineArr[3]
							+ "\t$$value$$"));
					// System.out.println(key+"\t"+value);

				}
			}

		}
	}

	public static void getClusterNumber(String args[]) throws IOException {

		Path inputPath = new Path(args[1]);// Input file name
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(inputPath)));

		String line;
		// line = br.readLine();
		while ((line = br.readLine()) != null) {
			if (line.contains("Node_" + searchIndex)) {
				int count=line.split(":").length;
				String arr[]=line.split(":");
				boolean flag=false;
				for(int i=0;i<count-1;i++){
					if(flag){
						clusterNumber+=":";
						clusterNumber =clusterNumber+ arr[i];
						
					}
					else{
						flag=true;
						clusterNumber =arr[i];
						
					}
				}
				//clusterNumber = line.split(":")[0];
				break;
			}
		}
		br.close();

	}

	public static String searchIndex(String args[]) throws IOException {
		System.out.println("Enter the title of paper you want to search ");

		Scanner sc = new Scanner(System.in);
		searchTitle = sc.nextLine().toLowerCase();

		Path inputPath = new Path(args[0]);// Input file name
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(inputPath)));

		String line;
		// line = br.readLine();
		while ((line = br.readLine()) != null) {
			String lineArr[] = line.split(Constant.SEPARATOR_TAB);
			if (lineArr.length > 1
					&& lineArr[1].toLowerCase().contains(searchTitle)) {
				searchIndex = line.split(Constant.SEPARATOR_TAB)[0];
				searchTitle=lineArr[1];
				break;
			}
		}
		br.close();
		return searchTitle;
	}

}
