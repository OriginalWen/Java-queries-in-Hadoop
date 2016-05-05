package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class query2 {
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	//private final static IntWritable one = new IntWritable(1);
	private Text id = new Text();
	private Text totaltransaction= new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		String line = value.toString();

	   	String[] attributes=line.split(",");
		id.set(attributes[1]);

		Float Transtotal=Float.parseFloat(attributes[2]);
		totaltransaction.set(String.valueOf(Transtotal));
		output.collect(id, totaltransaction);



	    //StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
	    //while (tokenizerArticle.hasMoreTokens()) {
		//StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken(),",");

		//tokenizerLine.nextToken();
		//word.set(tokenizerLine.nextToken());

		//output.collect(word, one);
	    //}
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	int numoftrans = 0;
	    	float totaloftrans = 0.0f;
	    	while (values.hasNext()) {
			String number = values.next().toString();
			totaloftrans += Float.parseFloat(number);
			numoftrans+=1;
	   	}
		String transnum= String.valueOf(numoftrans);
		output.collect(new Text(key), new Text(",  " + String.valueOf(numoftrans) + "  ,  " + String.valueOf(totaloftrans)));
	}
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(query2.class);
	conf.setJobName("query2");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }
}
