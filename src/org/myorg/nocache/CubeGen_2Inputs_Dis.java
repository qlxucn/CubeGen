
package org.myorg.nocache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.myorg.cached.CubeGenMapper;
import org.myorg.cached.CubeGenPartitioner;
import org.myorg.cached.CubeGenReducer;
import org.myorg.cached.CubeGenReducer_NonDis;
import org.myorg.cached.OutputFormat_NoName;
import org.myorg.cached.OutputFormat_WithName;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.NAME_BATCHID;

public class CubeGen_2Inputs_Dis extends Configured implements Tool {

	private Global m_global = null;

	@SuppressWarnings("deprecation")
	private void runGenJob(String jobName, String sIns, Path pOut,
			boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? CubeGenReducer.class
				: CubeGenReducer_NonDis.class;

		JobConf conf = new JobConf(getConf(), CubeGen_2Inputs_Dis.class);
		conf.setJobName(jobName);

		conf.setInt("Column.Count", Common.column_count);
		conf.setNumReduceTasks(Common.reduce_task_num);
		conf.setJobOperator(isDistributive?"sum":"median");
		
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

		conf.setOutputFormat(OutputFormat_NoName.class);
		conf.setMapperClass(CubeGenMapper.class);
		conf.setPartitionerClass(CubeGenPartitioner.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(reduceClass);

		FileInputFormat.setInputPaths(conf, sIns);
		FileOutputFormat.setOutputPath(conf, pOut);

		JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception {
//		boolean isDistributive = args[0].equals("0");
		
		// Get pathes
		Path pOriginInput = new Path(args[1]);
		Path pOriginOutput = new Path(args[2]);
		Path pExtraInput = new Path(args[3]);
//		Path pExtraOutput = new Path(args[4]);
//		Path pUpdateOutput = new Path(args[5]);

		runGenJob("CubeNoCache_2Inputs_Dis_1", 
				pOriginInput.toString() + "," + pExtraInput.toString(),
				pOriginOutput, true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime;
		long endTime;
		long totalTime;
		startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new CubeGen_2Inputs_Dis(), args);
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
