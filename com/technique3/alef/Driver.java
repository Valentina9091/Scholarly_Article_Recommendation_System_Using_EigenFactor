package com.technique3.alef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This the the main driver class in which we compute scholarly recommended
 * article algorithm's step1 and step2. The final output of this driver class is
 * the ALEF scores.
 *
 * 
 * @author Valentina Palghadmal
 *
 */
public class Driver {

	final static int count = 2092356;
	private static final Logger LOG = Logger.getLogger(Driver.class);

	// static int total = 1000000;// 2092356;

	public static void main(String[] args) throws Exception {
		int res = 0;

		Configuration conf = new Configuration(true);
		// Step1
		// To check whether input and output destinations are provided.e
		if (args.length < 2) {
			LOG.error("Missing arguments. There should be two arguments source and destination folder");
		}
		// Execution call for link graph method
		res = ToolRunner.run(conf, new LinkGraph(), args);
		LOG.info("LinkGraph executed successfully");
		if (res == 0) {
			// Execution call for addition of Zij and Zij transpose matrix
			res = ToolRunner.run(new AdditionOfMatrix(), args);
			LOG.info("Addition of matrices executed successfully");
			// execution for forming W vector
			if (res == 0) {
				res = ToolRunner.run(new WMatrix(), args);
				LOG.info("W vector formed successfully");
				if (res == 0) {
					// execution for forming row scholastic matrix
					res = ToolRunner.run(new HMatrix(), args);
					LOG.info("H matrix formed successfully");
					if (res == 0) {
						// Simplifying vector for matrix multiplication
						res = ToolRunner.run(new W_Modified(), args);
						LOG.info("Simplification done successfully");
						if (res == 0) {
							 Executing matrix multiplication
							res = ToolRunner.run(new DotProduct(), args);
							LOG.info(" matrix formed successfully");
							if (res == 0) {
								// Executing alef
								res = ToolRunner.run(new ALEF(), args);
								//LOG.info("ALEF scored computed successfully");
								if(res==0){
									//map links
									res=ToolRunner.run(new MapequationInputPrep(), args);
									if(res==0){
										res=ToolRunner.run(new MapEquation_pajek(), args);
								if (res == 0) {
									System.exit(res);
								}
							}}}
						}
					}

				}
			}
		}
	}
}
