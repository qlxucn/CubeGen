package org.myorg.naive;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;

public class CubeNaiveReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int sum;
	private IntWritable value = new IntWritable();

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// sum up the values
		sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}

		value.set(sum);
		output.collect(key, value);
	}

}
