package com.hw1bigram1A;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hw1bigram1A.Bigram1A;
import com.hw1bigram1A.Bigram1AMapper;
import com.hw1bigram1A.Bigram1AReducer;
import com.wholefileinputformat.WholeFileInputFormat;

public class Bigram1A {
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
		      System.exit(-1);
		    }
		
		Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Bigram1A.class);



        job.setMapperClass(Bigram1AMapper.class);
        //job.setCombinerClass(Unigram1AReducer.class);
        job.setReducerClass(Bigram1AReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(40);

        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
