package org.myorg.nocache;

import java.io.IOException;
import java.util.BitSet;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;

public class CubeUpdateMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void configure(JobConf jobConf) {

	}

	private String name_key = new String();
	private String tmp = new String();
	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable();
	private StringTokenizer itr = null;

	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		name_key = "";
		// Split the name_key, value
		itr = new StringTokenizer(value.toString(), Common.splitSeperator);
		if (itr.countTokens() < 2) {
			System.out.println("CubeUpdateMapper::map():Tokens num error.");
			return;
		}

		while (itr.hasMoreTokens()) {
			tmp = itr.nextToken();
			if (!itr.hasMoreTokens()) {
				outValue.set(Integer.valueOf(tmp));
			} else {
				name_key += tmp + Common.splitSeperator;
			}
		}

		outKey.set(name_key);

		output.collect(outKey, outValue);
	}
}
