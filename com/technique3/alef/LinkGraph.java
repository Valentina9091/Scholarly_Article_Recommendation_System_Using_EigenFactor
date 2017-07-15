package com.technique3.alef;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class forms the link graph. Input for this class in form of mutliple
 * lines.
 * 
 * The first step requires assembling the citation graph for the large corpus.
 * The graph is represented as a link list file. Key : Map Output:- Emit Key:
 * referencePaperID Value: paperID Reduce Output: Emit Key:
 * referencePaperID,paperID Value: 1###count
 * 
 * 1 – indicates that reference paper is cited in that paper id. Used in later
 * stages for matrix manipulations Count – the total number of papers in which
 * the current paper is cited. The output of this phase is Zij matrix. The
 * matrix is highly sparse since an individual article cites a tiny portion of
 * all articles in the corpus. So we form the following structure in MR jobs for
 * better performance.
 * 
 * 
 * @author Valentina Palghadmal
 *
 */
public class LinkGraph extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(LinkGraph.class
			.getName());
	private static final Pattern LINE_PATTERN = Pattern.compile("\n");
	private static final Pattern SPACE_PATTERN = Pattern.compile("\\s+");
	private static final String CITATION_ID_IDENTIFIER = "#%";

	public int run(String[] args) throws Exception {
		// Delete if output directory exists
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(new Path(args[1] + Constant.OUTPUT_DIR_LINK_GRAPH))) {
			fs.delete(new Path(args[1] + Constant.OUTPUT_DIR_LINK_GRAPH), true);
		}

		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter", "#index");

		Job job = Job.getInstance(getConf(), "LinkGraph");
		job.setJarByClass(this.getClass());

		// The input and output be the sequence file i/o
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]
				+ Constant.OUTPUT_DIR_LINK_GRAPH));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String key = null;
			String[] rowArr = null;
			String line = lineText.toString();
			String[] recArr = LINE_PATTERN.split(line);
			String value = recArr[0];
			for (String record : recArr) {
				if (record.startsWith(CITATION_ID_IDENTIFIER)) {
					rowArr = SPACE_PATTERN.split(record);
					key = rowArr[1];
					context.write(new IntWritable(Integer.valueOf(key)),
							new Text(value));
				}
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
		@Override
		public void reduce(IntWritable keyText, Iterable<Text> valueList,
				Context context) throws IOException, InterruptedException {
			StringBuffer valueBuffer = new StringBuffer();
			int counter = 0;
			for (Text value : valueList) {
				if (counter > 0) {
					valueBuffer.append(",");
					valueBuffer.append(value.toString().trim());
				} else {
					valueBuffer.append(value.toString().trim());
				}
				counter++;

				// LOG.info("saved " + counter);
			}

			if ((valueBuffer.toString()).contains(",")) {

				String ab[] = (valueBuffer.toString()).split(",");

				for (int i = 0; i < ab.length; i++) {
					// LOG.info(ab[i] + "at the second loop");
					context.write(new Text(keyText + "," + ab[i]), new Text("1"
							+ "###" + counter));
				}
			} else {
				context.write(new Text(keyText + "," + valueBuffer.toString()),
						new Text("1" + "###" + counter));
			}
		}
	}

}
