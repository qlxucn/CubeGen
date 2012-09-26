package org.myorg.nocache;

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

public class CubeUpdateReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, Text> {

	private String name = new String();
	private String key_value = new String();
	private int sum;
	private Text outKey = new Text();
	private Text outValue = new Text();
	private StringTokenizer itr = null;

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// split 'name' and 'key'
		itr = new StringTokenizer(key.toString(), "\t");
		if (itr.countTokens() < 2) {
			System.out
					.println("CubeUpdateReducer::reduce():Can NOT split 'name' and 'key'");
			return;
		}
		name = itr.nextToken();
		key_value = itr.nextToken();

		// sum up the values
		sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}

		// get 'key_value'
		key_value += Common.splitSeperator + String.valueOf(sum);

		outKey.set(name);
		outValue.set(key_value);
		output.collect(outKey, outValue);
	}

}
