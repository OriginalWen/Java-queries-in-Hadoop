package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class query1 {
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    	String line = value.toString();

	   	String[] attributes=line.split(",");
		int countrycode=Integer.parseInt(attributes[3]);
		if(countrycode>=2 && countrycode<=6){
			word.set(attributes[0]);
			output.collect(word, one);
		}

	    //while (tokenizerArticle.hasMoreTokens()) {
		//StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken(),",");

		//word.set(tokenizerLine.nextToken());
		//tokenizerLine.nextToken();
		//tokenizerLine.nextToken();
		//cc.set(tokenizerLine.nextToken());
		//output.collect(word, cc);
		
	    
	}
    }



    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(query1.class);
	conf.setJobName("query1");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	//conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }
}
