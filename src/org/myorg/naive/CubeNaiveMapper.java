package org.myorg.naive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
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

public class CubeNaiveMapper extends MapReduceBase
		implements
			Mapper<Object, Text, Text, IntWritable> {

	private Integer[] cols = null;
	private String tmp = new String();
	private String[] parts = null;
	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable();
	private StringTokenizer itr = null;

	@Override
	public void configure(JobConf jobConf) {
		cols = getColsByCuboidName(jobConf.get("Cuboid.Name"));
		// every input value contains 'Column.Count' keys and one value
		int eleNum = jobConf.getInt("Column.Count", -1) + 1;
		this.parts = new String[eleNum];
	}

	/**
	 * get the column ids by cuboid name; eg:"0_1_2_" -> {0, 1, 2}
	 * 
	 * @param name
	 *            cuboid name
	 * @return an array of column ids
	 */
	private Integer[] getColsByCuboidName(String name) {

		List<Integer> colList = new ArrayList<Integer>();
		StringTokenizer itr2 = new StringTokenizer(name.toString(),
				Common.nameSeperator);
		while (itr2.hasMoreTokens()) {
			colList.add(Integer.valueOf(itr2.nextToken()));
		}

		return colList.toArray(new Integer[0]);
	}

	int index;
	String tmpKey = new String();
	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		// Split the name_key, value
		itr = new StringTokenizer(value.toString(), Common.splitSeperator);
		if (itr.countTokens() != parts.length) {
			System.out
					.println("CubeNaiveMapper::map():Tokens num error. token num="
							+ itr.countTokens()
							+ " parts.length="
							+ parts.length);
			return;
		}

		// clear parts
		for (int i = 0; i < parts.length; i++) {
			parts[i] = "";
		}

		// generate parts
		index = 0;
		while (itr.hasMoreTokens()) {
			parts[index++] = itr.nextToken();
		}

		// generate the key by cuboid name
		tmpKey = "";
		for (Integer col : cols) {
			tmpKey += parts[col] + Common.splitSeperator;
		}

		outKey.set(tmpKey);
		// the last element in 'parts[]'
		outValue.set(Integer.valueOf(parts[parts.length - 1]));

		output.collect(outKey, outValue);
	}
}
