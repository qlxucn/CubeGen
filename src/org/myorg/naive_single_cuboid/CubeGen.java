/**
 * handle one cuboid for each launch
 * NO USE NOW*
 * **/

package org.myorg.naive_single_cuboid;

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

import org.myorg.naive.CubeNaiveMapper;
import org.myorg.naive.CubeNaiveReducer;
import org.myorg.naive.CubeNaiveReducer_NonDis;

public class CubeGen extends Configured implements Tool {

	private Global m_global = null;

	private void runGenJob(Path pIn, Path pOut,
			Integer cuboid, boolean isDistributive) throws IOException {
		// choose reduce class according to 'isDistributive'
		Class<? extends Reducer> reduceClass = isDistributive
				? CubeNaiveReducer.class
				: CubeNaiveReducer_NonDis.class;
		String operator = isDistributive ? "Dis" : "NonDis";

		JobConf conf = new JobConf(getConf(), CubeGen.class);
	
		conf.setInt("Column.Count", Common.column_count);
		conf = Global.setJobEnv(conf);
		
		m_global = new Global();
		m_global.initGlobal(conf);
		String cuboidName = m_global.cubeInfoByCuboId.get(cuboid).name;
		conf.set("Cuboid.Name", cuboidName);

		conf.setJobName("CubeNaive_" + operator + "_" + cuboidName);
		Path pOutSub = new Path(pOut, cuboidName);
		
		int reduceNum = (int) Math.round(reduce_weights[cuboid] * 280);
		conf.setNumReduceTasks(reduceNum);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CubeNaiveMapper.class);
		conf.setReducerClass(reduceClass);

		FileInputFormat.setInputPaths(conf, pIn);
		FileOutputFormat.setOutputPath(conf, pOutSub);

		JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception {
		boolean isDistributive = args[0].equals("0");
		String sInput = args[1];
		String sOutput = args[2];
		Integer cuboid = Integer.valueOf(args[3]);
		
		Path pIn = new Path(sInput);
		Path pOut = new Path(sOutput);
		this.runGenJob(pIn, pOut, cuboid, isDistributive);

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
	
	private double[] reduce_weights = {
			0.041531823085221145,
			0.06796116504854369,
			0.09398597626752966,
			0.0877831715210356,
			0.050431499460625674,
			0.0703883495145631,
			0.09479503775620281,
			0.034385113268608415,
			0.06216289104638619,
			0.07241100323624595,
			0.057982740021574976,
			0.045307443365695796,
			0.059735706580366775,
			0.06634304207119741,
			0.09479503775620281
	};
}
