package org.myorg.cached;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;
import org.myorg.common.Global;

public class CubeGenCombiner extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private int sum;
	private int batchid;
	private String value_batchid;
	private Global m_global = new Global();
	private Text m_valueOut = new Text();

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		if (!values.hasNext()) {
			System.out.println(key.toString() + "= No values");
			return;
		}
		
		sum = 0;
		while(values.hasNext()){
			value_batchid = values.next().toString();
			sum += m_global.getFirstHalf(value_batchid);		
		}
		
		batchid = m_global.getLastHalf(value_batchid);
		
		m_valueOut.set(String.valueOf(sum) + Common.value_batchidSeperator + String.valueOf(batchid));
		output.collect(key, m_valueOut);
	}
}
