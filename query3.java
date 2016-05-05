package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class query3 {  

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {  
           
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {  
               
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();  
              
            String line = value.toString();  
            
            //if (line == null || line.equals("")) return;   
              
              
            if (filePath.contains("Customers")) {  
                String[] values = line.split(","); //                    
                  
                String id = values[0]; // id  
                String name = values[1]; // name  
                //String age = values[2];//age
                //String countrycode = values[3]; //country_code
                String salary = values[4];
                             
                output.collect(new Text(id), new Text("a#"+name+","+salary));  
            }  
            // B  
            else if (filePath.contains("Transactions")) {  
                String[] values = line.split(","); //   
         
                //String tID = values[0]; // Transaction id  
                String cID = values[1]; // Customer id
                String tTotal = values[2]; //transaction Total  
                String tNumItems = values[3];//transaction Number of Items              
                
                output.collect(new Text(cID), new Text("b#"+tTotal+","+tNumItems));  
            }  
        }  
    }  
      
//     reduce  
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {  
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {  
                      
            Vector<String> vecA = new Vector<String>(); // Store A's contents  
            Vector<String> vecB = new Vector<String>(); // Store B's contents
              
            while (values.hasNext()) {  
                String value = values.next().toString();  
                if (value.startsWith("a#")) {  
                    vecA.add(value.substring(2));  
                } else if (value.startsWith("b#")) {  
                    vecB.add(value.substring(2));  
                }  
            }  
              
            //int sizeA = vecA.size();  
            int sizeB = vecB.size();  
             
            int NumOfTransactions=0;
            float TotalSum=0.0f;
            int Minitems=10;
            for (int j = 0; j < sizeB; j ++) {
            	NumOfTransactions +=1;
            	
            	String[] attributes=vecB.get(j).split(",");
            	float Temp=Float.parseFloat(attributes[0]);
            	TotalSum += Temp;
            	int Currentitems=Integer.parseInt(attributes[1]);
            	if (Minitems>Currentitems){
            		Minitems=Currentitems;
            	}
            	
            }
            output.collect(key, new Text(",  " +vecA.get(0) + "  ,  " +String.valueOf(NumOfTransactions)+"  ,  "+String.valueOf(TotalSum)+"  ,  "+String.valueOf(Minitems)));
            
        }  
    }  
      
//    protected void configJob(JobConf conf) {  
//        conf.setMapOutputKeyClass(Text.class);  
//        conf.setMapOutputValueClass(Text.class);  
//        conf.setOutputKeyClass(Text.class);  
//        conf.setOutputValueClass(Text.class);  
//        conf.setOutputFormat(ReportOutFormat.class); 
        
        
        public static void main(String[] args) throws Exception {
            JobConf conf = new JobConf(query3.class);
            conf.setJobName("query3");

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
