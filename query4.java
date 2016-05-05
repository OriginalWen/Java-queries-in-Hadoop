//package org.myorg;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.DistributedCache;

public class query4 {
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    	private Map<String, String> countryCodeMap = new HashMap<String, String>();
    	//we wanna do a map side join to make the job done in one mapreduce
    	//need store the smaller file in main memory to join in map side
    	@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;    
            try {
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());		//here we stored the customers.txt in memory
                String customer = null;
                for (Path path : paths) {
                    if (path.toString().contains("Customers")) {
                        in = new BufferedReader(new FileReader(path.toString()));	//readin the customers 
                        while (null != (customer = in.readLine())) {
                        	String[] distributes = customer.split(",");
                        	countryCodeMap.put(distributes[0], distributes[3]);	//leave only id and total transactions
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    	 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	 
            String[] line = value.toString().split(","); 
		//here we get the countrycode as the key corresponding to each id in transaction.txt, so the value is id and TransTotal combined    
            context.write(new Text(countryCodeMap.get(line[1].trim())), new Text(line[1].trim() + "," + line[2].trim()));

        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 
            float minTransTotal = 1000.0f;
            float maxTransTotal = 0.0f;
	    List<Integer> idnum = new ArrayList<Integer>();
        	for (Text value : values) {
                String[] val = value.toString().split(",");  
                int id = Integer.parseInt(val[0]);
                float tt = Float.parseFloat(val[1]);		
                if (tt < minTransTotal) { minTransTotal = tt;}
                else if (tt > maxTransTotal) { maxTransTotal = tt;}
                if (!idnum.contains(id)) {idnum.add(id);}
            }  
            context.write(key, new Text(",  " +String.valueOf(idnum.size()) + "  ,  " + String.valueOf(minTransTotal) + "  ,  " + String.valueOf(maxTransTotal)));
        }
    }
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "query4");
        job.setJobName("query4");

        job.setJarByClass(query4.class);
        job.setMapperClass(MapClass.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
  
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
