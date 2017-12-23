package com.hw1bigram1A;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Bigram1AReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		Set<String> s = new TreeSet<String>();
		
		for (Text val : values) {
			s.add(val.toString());
			sum += 1 ;
		}
		
		context.write(key, new Text(sum + "\t" + s.size()));
	}
}
