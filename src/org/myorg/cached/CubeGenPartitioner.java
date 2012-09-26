package org.myorg.cached;

import java.util.BitSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.myorg.common.Common;
import org.myorg.common.Global;
import org.myorg.common.Global.RANGE;

public class CubeGenPartitioner implements Partitioner<Text, Text> {
	private Global m_global = new Global();

	@Override
	public void configure(JobConf job) {
		m_global.initGlobal(job);
	}

	private String firstPart = new String();
	private StringTokenizer itr = null;
	private RANGE range = null;
	private int batchId;
	@Override
	public int getPartition(Text key, Text value, int numReduceTask) {
		batchId = m_global.getLastHalf(value.toString());
		range = m_global.cubeInfoByBatchId.get(batchId).range;

		// splitKey(key.toString(), ms_splitNum);
		itr = new StringTokenizer(key.toString(),
				Common.splitSeperator);
		if (itr.countTokens() == 0) {
			System.out.println("getPartition() error: nTokenNum is zero");
			return 0;
		}
		firstPart = itr.nextToken();

		int reduceNo = (range.start + Math.abs(firstPart.hashCode()) % range.span);
//		if (reduceNo < 0) {
//			System.out.println("getPartition: key = " + key.toString()
//					+ "  partition = " + String.valueOf(reduceNo)
//			+ "  start = " + range.start
//			+ "  firstPart = " + firstPart
//			+ "  firstPart.hashCode() = " + firstPart.hashCode()
//			+ "  span = " + range.span);
//		}

		return reduceNo;
	}
}
