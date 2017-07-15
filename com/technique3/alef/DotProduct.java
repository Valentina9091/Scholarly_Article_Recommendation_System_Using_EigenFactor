package com.technique3.alef;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This class computes multiples HijT with Wi using a MR job and write it to an
 * output file.
 * 
 * @author Valentina Palghadmal
 *
 */
public class DotProduct extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(DotProduct.class.getName());

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		FileSystem hdfs = FileSystem.get(config);
		Path output = new Path(args[1] + Constant.OUTPUT_DIR_DOT_PRODUCT);
		// delete existing directory
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_LINK_GRAPH), true);
		// hdfs.delete(new Path(args[1]+Constant.OUTPUT_DIR_ADD), true);

		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		Job job = Job.getInstance(getConf(), "Dot Product");
		job.setJarByClass(this.getClass());
		MultipleInputs.addInputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_H), TextInputFormat.class, Map.class);
		MultipleInputs.addInputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_W_MOD), TextInputFormat.class, Map.class);
		FileOutputFormat.setOutputPath(job, output);
		job.getConfiguration()
				.setStrings(Constant.ROW_COUNT, Driver.count + "");
		job.getConfiguration().setStrings(Constant.INTERMEDIATE_COL_ROWS,
				Driver.count + "");
		job.getConfiguration().setStrings(Constant.COLUMN_COUNT, "1");
		job.setReducerClass(Reduce.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		int noOfRows, noOfCols;

		public void setup(Context context) throws IOException,
				InterruptedException {
			noOfRows = context.getConfiguration().getInt(Constant.ROW_COUNT, 1);
			noOfCols = context.getConfiguration().getInt(Constant.COLUMN_COUNT,
					1);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] indicesAndValue = value.toString().split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
			if (indicesAndValue[0].equals(Constant.ROW_STOCHASTIC_MATRIX)) {
				for (int k = 1; k <= noOfCols; k++) {
					outputKey.set(Integer.parseInt(indicesAndValue[1]) + ","
							+ k);
					outputValue.set(indicesAndValue[0] + ","
							+ indicesAndValue[2] + "," + indicesAndValue[3]);
					System.out.println("Map H ::: " + outputKey + "\t"
							+ outputValue);

					context.write(outputKey, outputValue);
				}
			} else {
				String rows[] = indicesAndValue[3].split(Constant.SEPARATOR);
				for (int i = 1; i < rows.length; i++) {
					outputKey.set(rows[i] + "," + indicesAndValue[2]);
					outputValue.set(Constant.TELEPORTATION_WEIGHT + ","
							+ indicesAndValue[1] + "," + rows[0]);
					System.out.println("Map W ::: " + outputKey + "\t"
							+ outputValue);
					context.write(outputKey, outputValue);

				}
				// System.out.println("MApper ::: "+outputKey+"\t"+outputValue);
				/*
				 * for (int i = 1; i <= noOfRows; i++) { outputKey.set(i + "," +
				 * indicesAndValue[2]);
				 * outputValue.set(Constant.TELEPORTATION_WEIGHT + "," +
				 * indicesAndValue[1] + "," + indicesAndValue[3]);
				 * context.write(outputKey, outputValue); }
				 */
			}

		}
	}

	// Reducer Function computes word count and term frequency accordingly
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		int intermediate;

		public void setup(Context context) throws IOException,
				InterruptedException {
			intermediate = context.getConfiguration().getInt(
					Constant.INTERMEDIATE_COL_ROWS, 1);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] value;

			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
			for (Text val : values) {
				value = val.toString().split(",");
				if (value[0].equals(Constant.ROW_STOCHASTIC_MATRIX)) {
					hashA.put(Integer.parseInt(value[1]),
							Float.parseFloat(value[2]));
				} else {
					hashB.put(Integer.parseInt(value[1]),
							Float.parseFloat(value[2]));
				}
			}
			// int n = Integer.parseInt(context.getConfiguration().get("n"));
			float result = 0.0f;
			float m_ij;
			float n_jk;
			for (int j = 1; j <= intermediate; j++) {
				m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
				n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
				result += m_ij * n_jk;
			}
			// Save only non zero values
			if (result != 0.0f) {
				context.write(null,
						new Text(key.toString() + "," + Float.toString(result)));
			}
		}
	}

}
