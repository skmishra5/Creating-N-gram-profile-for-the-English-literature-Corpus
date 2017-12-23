package com.hw1unigram1B;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Unigram1BMapper extends Mapper<Object, Text, Text, Text>{
	private String author = new String("");
	private String title = new String("");
	private String buf;
	
	private boolean book_start;
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = "";
		String total = value.toString().toLowerCase();
		BufferedReader rd = new BufferedReader(new StringReader(total));
		while((line = rd.readLine()) != null)
		{
		    StringTokenizer itr = new StringTokenizer(line.toString());
		
		    if(book_start){				
			    while (itr.hasMoreElements()) {
				    String s = (String)itr.nextElement();
				    s=s.toLowerCase().replaceAll("[^A-Za-z]", "");
				    if(s.length() != 0) {
					    buf = s + '\t' + author;
					    //word.set(buf);
					    context.write(new Text(buf), new Text(title));;
				    }
			    }
		    }
		
		    else if(line.startsWith("author:")){
			    author = line.substring(line.lastIndexOf(" ") + 1);
			    author.toLowerCase().replaceAll("[^A-Za-z]", "");
			
		    }
		    else if(line.startsWith("*** start of this")){
			    book_start = true;				
		    }
		    else if(line.startsWith("title:")){	
			    title = line.substring(line.indexOf("title:") + 7);
									
		    }
		
		
		 }
	}

}
