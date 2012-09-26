package org.myorg.cached;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class OutputFormat_WithName extends
		MultipleTextOutputFormat<Text, Text> {

	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String name) {

		return key.toString() + "/" + name;
	}

}
