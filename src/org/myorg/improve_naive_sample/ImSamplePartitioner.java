package org.myorg.improve_naive_sample;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.myorg.common.Common;
import org.myorg.common.Global;

public class ImSamplePartitioner implements Partitioner<Text, Text> {
	private Global m_global = new Global();

	@Override
	public void configure(JobConf job) {
		m_global.initGlobal(job);
	}

	private int cuboid;
	@Override
	public int getPartition(Text key, Text value, int numReduceTask) {
		cuboid = m_global.getLastHalf(value.toString());
		return cuboid % numReduceTask;
	}

}
