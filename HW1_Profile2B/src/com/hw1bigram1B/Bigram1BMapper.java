package com.hw1bigram1B;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Bigram1BMapper extends Mapper<Object, Text, Text, Text>{
	private String author = new String("");
	private String title = new String("");
	private String buf = new String("");
	private String buf1, buf2;
	
	private boolean book_start;
	private Text word = new Text();
	
	private String st = new String("_");
	
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
				    s=s.toLowerCase().replaceAll("[^A-Za-z\\.]", "");
				    if(s.length() != 0) {
				    	if(s.contains(".")){
					    	StringTokenizer stop = new StringTokenizer(s, ".");
					    	while(stop.hasMoreTokens()){
					    		s = stop.nextToken();
					    		buf = st + " " + s;
							    buf1 = buf + '\t' + author;
							    context.write(new Text(buf1), new Text(title));
							    st = s;
					    	}
					    	buf2 = st + "\t" + "_" + "\t" + author;
							context.write(new Text(buf2), new Text(title));
							st = "_";
					    }
				    	else{
					    buf = st + "  " + s;
					    buf1 = buf + '\t' + author;
					    context.write(new Text(buf1), new Text(title));
					    st = s;
					    }
				    }
			    }
			    //buf2 = st + "\t" + "_" + "\t" + author;
				//context.write(new Text(buf2), new Text(title));
		    }
		
		    else if(line.startsWith("author:")){
			    author = line.substring(line.lastIndexOf(" ") + 1);
			    author.replaceAll("[^A-Za-z]", "");
			
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
