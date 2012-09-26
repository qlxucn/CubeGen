/**
 * Generate the cubes from the raw data.
 * Raw data like : (X, Y, Z, A); 'A' is the value of (X, Y, Z)
 * 1st: Generate cube1:(X, A), cube2:(X, Y, A), cube3:(X, Y, Z, A)
 * 2nd: Merge cube and cube'; both of them are in same format like (X, A)
 * 
 * X(0-bit), Y(1-bit), Z(2-bit), XY(3-bit), YZ(4-bit), ZX(5-bit), XYZ(6-bit)
 * e.g: {X, XY, XYZ} can be tagged "0100 1001", that is "0x49"
 * **/

package org.myorg.naive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
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

import org.myorg.common.Common;
import org.myorg.common.Global;

public class CubeGen extends Configured implements Tool {

	private Global m_global = null;

	private void runGenJob(String jobName, Path pIn, Path pOut,
			String cuboidName, boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? CubeNaiveReducer.class
				: CubeNaiveReducer_NonDis.class;

		JobConf conf = new JobConf(getConf(), CubeGen.class);

		conf.setJobName(jobName);
		conf.set("Cuboid.Name", cuboidName);
		conf.setInt("Column.Count", Common.column_count);

		conf.setNumReduceTasks(Common.reduce_task_num);

		conf = Global.setJobEnv(conf);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CubeNaiveMapper.class);
		conf.setReducerClass(reduceClass);

		FileInputFormat.setInputPaths(conf, pIn);
		FileOutputFormat.setOutputPath(conf, pOut);

		JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception {
		boolean isDistributive = args[0].equals("0");
		String sInput = args[1];
		String sOutput = args[2];

		// Initialize m_global
		JobConf confTmp = new JobConf(getConf(), CubeGen.class);
		confTmp.setInt("Column.Count", Common.column_count);
		confTmp.setNumReduceTasks(Common.reduce_task_num);
		m_global = new Global();
		m_global.initGlobal(confTmp);

		Iterator<Integer> itr = m_global.cubeInfoByCuboId.keySet().iterator();
		while (itr.hasNext()) {
			Integer cuboid = itr.next();
			String name = m_global.cubeInfoByCuboId.get(cuboid).name;
			Path pIn = new Path(sInput);
			Path pOut = new Path(sOutput, name);
			String operator = isDistributive ? "Dis" : "NonDis";
			
			if(cuboid == 3){
				this.runGenJob("CubeNaive_" + operator + "_" + name, pIn, pOut,
						name, isDistributive);	
			}
			
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
