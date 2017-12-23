package com.hw1unigram1A;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Unigram1AMapper extends Mapper<Object, Text, Text, Text> {
	
	private String year = new String("####");
	private String title = new String("asdf");
	private String buf = ""; // new String("");
	
	private boolean book_start = false;
	private Text word = new Text();
	HashSet<String> hs = new HashSet<String>();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = "";
		String total = value.toString().toLowerCase();
		//String line = value.toString().toLowerCase();
		//FileSplit fileSplit = (FileSplit)context.getInputSplit();
		//String filename = fileSplit.getPath().getName();
		BufferedReader rd = new BufferedReader(new StringReader(total));
		while((line = rd.readLine()) != null)
		{
		    StringTokenizer itr = new StringTokenizer(line.toString());
//		    if(hs.size()==0)
//			{
//				hs.add(filename);
//			
//			}
//			
//			if(!hs.contains(filename))
//			{
//				hs.add(filename);
//				book_start = false;
//			}
			
		    if(book_start){				
			    while (itr.hasMoreElements()) {
				    String s = (String)itr.nextElement();
				    s=s.toLowerCase().replaceAll("[^A-Za-z]", "");
				    if(s.length() != 0) {
					    buf = s + '\t' + year;
					    //String values=filename+" "+1;
//					    word.set(buf);
					    context.write(new Text(buf), new Text(title));;
				    }
			    }
//			    book_
		    }
		
		    else if(line.startsWith("release date:")){
			    int year_start = line.indexOf(",") + 2; 
			    if(year_start > 0 &&year_start < line.length() && year_start+4 <= line.length())
			    	year = line.substring(year_start,year_start+4);
			
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