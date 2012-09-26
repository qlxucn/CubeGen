package org.myorg.cached;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.RANGE;

public class CubeGenPartitioner_Sample implements Partitioner<Text, Text> {
	private Global m_global = new Global();

	@Override
	public void configure(JobConf job) {
		m_global.initGlobal(job);
	}

	private int batchId;
	@Override
	public int getPartition(Text key, Text value, int numReduceTask) {
		batchId = m_global.getLastHalf(value.toString());
		return batchId;
	}
}