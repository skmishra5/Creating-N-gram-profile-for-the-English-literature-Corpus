package com.wholefileinputformat;

import java.io.IOException;

import com.wholefilerecordreader.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat <Object, Text>{
	 @Override 
	    public RecordReader<Object, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) 
	            throws IOException, InterruptedException { 
	        return new WholeFileRecordReader(); 
	    } 
	 
	    @Override 
	    protected boolean isSplitable(JobContext context, Path filename) { 
	        return false; 
	    }
}
