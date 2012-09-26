package org.myorg.batched_sample;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.myorg.cached.CubeGenMapper;
import org.myorg.cached.CubeGenPartitioner_Sample;
import org.myorg.cached.CubeGenReducer;
import org.myorg.cached.CubeGenReducer_NonDis;
import org.myorg.cached.OutputFormat_WithName;
import org.myorg.cached.CubeGenCombiner;
import org.myorg.common.BatchGen_Weight;
import org.myorg.common.Common;
import org.myorg.common.Global;

public class CubeGen extends Configured implements Tool {

	private Global m_global = null; 

	@SuppressWarnings("deprecation")
	private void runGenJob(String jobName, Path pIn, Path pOut,
			boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? CubeGenReducer.class
				: CubeGenReducer_NonDis.class;

		JobConf conf = new JobConf(getConf(), CubeGen.class);
		conf.setJobName(jobName);
		conf.setInt("Column.Count", Common.column_count);
		
		//Get batch count
		BatchGen_Weight bg = new BatchGen_Weight();
		bg.create(Common.column_count);
		conf.setJobOperator(isDistributive?"sum":"median");
		
		//Ensure every batch get only one reducer to run
		conf.setNumReduceTasks(bg.getBatchCount());

		conf = Global.setJobEnv(conf);
		
		// Initial 'global'
		if (m_global == null) {
			m_global = new Global();
			m_global.initGlobal(conf);
			
			// Print out batch schedule info
			m_global.printBatchInfo();
		}
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputFormat(OutputFormat_WithName.class);
		conf.setMapperClass(CubeGenMapper.class);
		conf.setPartitionerClass(CubeGenPartitioner_Sample.class);
		conf.setReducerClass(reduceClass);
		
		//No combiner for non-distributive
		if(isDistributive){
			conf.setCombinerClass(CubeGenCombiner.class);
		}

		FileInputFormat.setInputPaths(conf, pIn);
		FileOutputFormat.setOutputPath(conf, pOut);

		JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception {
		boolean isDistributive = args[0].equals("0");
		
		// Generate the original cube
		Path pOriginInput = new Path(args[1]);
		Path pOriginOutput = new Path(args[2]);
		String operator = isDistributive ? "Dis" : "NonDis";
		runGenJob("CubeBatched_Sample_" + operator, pOriginInput, pOriginOutput,
				isDistributive);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime;
		long endTime;
		long totalTime;
		startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new CubeGen(), args);
		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("The total execution time is: "
				+ printComputationDurationInSeconds(totalTime));
		System.exit(exitCode);
	}

	public static String printComputationDurationInSeconds(long duration) {
		long durationInSeconds = duration / 1000;
		String result = "Elapsed Time = " + durationInSeconds + " sec";
		return (result);

	}

}
