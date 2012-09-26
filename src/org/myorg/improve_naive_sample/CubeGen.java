package org.myorg.improve_naive_sample;

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

import org.myorg.cached.OutputFormat_NoName;
import org.myorg.common.BatchGen;
import org.myorg.common.Common;
import org.myorg.common.Global;

import org.myorg.improve_naive.ImproveNaiveMapper;
import org.myorg.improve_naive.ImproveNaiveReducer;
import org.myorg.improve_naive.ImproveNaiveReducer_NonDis;

@SuppressWarnings("deprecation")
public class CubeGen extends Configured implements Tool {

	private Global m_global = null;

	private void runGenJob(String jobName, Path pIn, Path pOut,
			boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? ImproveNaiveReducer.class
				: ImproveNaiveReducer_NonDis.class;

		JobConf conf = new JobConf(getConf(), CubeGen.class);
		conf.setJobName(jobName);
		conf.setInt("Column.Count", Common.column_count);
		conf.setJobOperator(isDistributive?"sum":"median");

		conf = Global.setJobEnv(conf);	
		
		//Ensure every cuboid get only one reducer to run
		int cuboidCount = (int) Math.pow(2, Common.column_count) - 1; //except the null cuboid
		conf.setNumReduceTasks(cuboidCount);
		
		// Initial 'global'
		if (m_global == null) {
			m_global = new Global();
			m_global.initGlobal(conf);
		}
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputFormat(OutputFormat_NoName.class);
		conf.setMapperClass(ImproveNaiveMapper.class);
		conf.setPartitionerClass(ImSamplePartitioner.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(reduceClass);

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
		runGenJob("CubeImproveNaive_" + operator, pOriginInput, pOriginOutput,
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
