/**
 * Generate the cubes from the raw data.
 * Raw data like : (X, Y, Z, A); 'A' is the value of (X, Y, Z)
 * 1st: Generate cube1:(X, A), cube2:(X, Y, A), cube3:(X, Y, Z, A)
 * 2nd: Merge cube and cube'; both of them are in same format like (X, A)
 * 
 * X(0-bit), Y(1-bit), Z(2-bit), XY(3-bit), YZ(4-bit), ZX(5-bit), XYZ(6-bit)
 * e.g: {X, XY, XYZ} can be tagged "0100 1001", that is "0x49"
 * **/

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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.myorg.cached.CubeGenMapper;
import org.myorg.cached.CubeGenPartitioner;
import org.myorg.cached.CubeGenReducer;
import org.myorg.cached.CubeGenReducer_NonDis;
import org.myorg.cached.OutputFormat_WithName;
import org.myorg.cached.OutputFormat_NoName;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.NAME_BATCHID;

public class CubeGen extends Configured implements Tool {

	private Global m_global = null;

	private void runGenJob(String jobName, String sIns, Path pOut,
			boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? CubeGenReducer.class
				: CubeGenReducer_NonDis.class;
		// choose output format class
		Class<? extends MultipleTextOutputFormat> outputFormatClass = isDistributive
				? OutputFormat_WithName.class
				: OutputFormat_NoName.class;

		JobConf conf = new JobConf(getConf(), CubeGen.class);
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

		conf.setOutputFormat(outputFormatClass);
		conf.setMapperClass(CubeGenMapper.class);
		conf.setPartitionerClass(CubeGenPartitioner.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(reduceClass);

		FileInputFormat.setInputPaths(conf, sIns);
		FileOutputFormat.setOutputPath(conf, pOut);

		JobClient.runJob(conf);
	}

	private void runUpdateJob(String jobName, String sIn1, String sIn2,
			Path pOut) throws IOException {
		JobConf conf = new JobConf(getConf(), CubeGen.class);
		conf.setJobName(jobName);
		
		conf.setNumReduceTasks(Common.reduce_task_num);
		
		conf = Global.setJobEnv(conf);

		conf.setMapOutputKeyClass(Text.class); // name + key
		conf.setMapOutputValueClass(IntWritable.class);// value
		conf.setOutputKeyClass(Text.class);// name
		conf.setOutputValueClass(Text.class);// key + value
		//
		conf.setOutputFormat(OutputFormat_WithName.class);
		conf.setMapperClass(CubeUpdateMapper.class);
		// conf.setPartitionerClass(CubeUpdatePartitioner.class);//No need
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(CubeUpdateReducer.class);

		// Add input paths
		Iterator<Integer> itr = m_global.cubeInfoByCuboId.keySet().iterator();
		while (itr.hasNext()) {
			Integer cuboId = itr.next();
			NAME_BATCHID name_batchid = m_global.cubeInfoByCuboId.get(cuboId);
			String sPaths = sIn1 + "/" + name_batchid.name + "," + sIn2 + "/"
					+ name_batchid.name;
			FileInputFormat.addInputPaths(conf, sPaths);
		}
		FileOutputFormat.setOutputPath(conf, pOut);

		JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception {
		boolean isDistributive = args[0].equals("0");

		// Get pathes
		Path pOriginInput = new Path(args[1]);
		Path pOriginOutput = new Path(args[2]);
		Path pExtraInput = new Path(args[3]);
		Path pExtraOutput = new Path(args[4]);
		Path pUpdateOutput = new Path(args[5]);

		if (isDistributive) {
			// Generate the original cube
			runGenJob("CubeNoCache_Dis_1", pOriginInput.toString(),
					pOriginOutput, true);
			// Generate the new cube
			runGenJob("CubeNoCache_Dis_2", pExtraInput.toString(),
					pExtraOutput, true);
			// Merge the new cube into the original cube
			runUpdateJob("CubeNoCache_Dis_3", pOriginOutput.toString(),
					pExtraOutput.toString(), pUpdateOutput);
		} else {
			// Generate from one inputs
			runGenJob("CubeNoCache_NonDis_1", pOriginInput.toString(),
					pOriginOutput, false);
			// Generate from two inputs
			runGenJob("CubeNoCache_NonDis_2", pOriginInput.toString()
					+ "," + pExtraInput.toString(), pExtraOutput,
					false);
		}

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
