package org.myorg.naive;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.myorg.common.Common;

public class CubeNaiveReducer_NonDis extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int med;
	private IntWritable value = new IntWritable();
	private Vector<Integer> vecVal = new Vector<Integer>();

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		//initialize <vecVal>
		vecVal.clear();

		while (values.hasNext()) {
			vecVal.add(values.next().get());
		}

		value.set(getMedium(vecVal));
		output.collect(key, value);
	}
	
	/**
	 * get the medium number in the list; 
	 * <br> {3,2,0,1,4} -> '2'
	 * <br> {3,2,0,5,1,4} -> '2'
	 * */
	private int getMedium(Vector<Integer> vec){
		Collections.sort(vec);
		return vec.get((int) Math.ceil((double)vec.size() / 2) - 1);
	}

}
