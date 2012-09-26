package org.myorg.cached;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class OutputFormat_NoName extends
		MultipleTextOutputFormat<Text, Text> {


	String nameTmp = new String();
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String name) {
		nameTmp = key.toString();//name
		key.set("");// Empty name
		
		return nameTmp + "/" + name;
	}

}
