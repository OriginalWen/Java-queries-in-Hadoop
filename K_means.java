package MyKmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class K_means {
    
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
	//We stored the seeds in two dimension list, the outer list is the point, the inner list represent each dimension of the point
	//We use arraylist so that it is suitable for three or higher dimension of points even though in our case it is simplely 2-dimension
	List<ArrayList<Float>> centers = null;	
        int k = 0;

        protected void setup(Context context) throws IOException,
                InterruptedException {
	    centers = Utils.getCenters("/Workspace/examples/Seeds");
            k = centers.size();	//We set k here as parameter, in our case k=10
        }

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
	    //We readin each line to the list fileds
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();	
	    //Since we are dealing with 2 dimension points, the sizeOfFileds would be 2

            double minDistance = 99999999;
            int centerIndex = 0;

            for(int i=0;i<k;i++){
                double currentDistance = 0;
                for(int j=0;j<sizeOfFileds;j++){
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double filed = Math.abs(fileds.get(j));
                    currentDistance += Math.pow((centerPoint - filed), 2);
                }
		//variable currentDistance here is actually current distance^2
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
	    //set key as the closest center, value still corresponding points
            context.write(new IntWritable(centerIndex+1), value);
	    //context.write(new Text(centers.get(centerIndex).toString()),value);
        }
        
    }

    public static class Combiner extends Reducer<IntWritable, Text, IntWritable, Text>{
	@Override
	
	protected void reduce(IntWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {

	    int numPoints=0;
            double[] agregPoints = {0.0, 0.0};
            for(Iterator<Text> it =value.iterator();it.hasNext();){
                ArrayList<Double> tempList = Utils.textToArray(it.next());
                agregPoints[0]+=tempList.get(0);
                agregPoints[1]+=tempList.get(1);
		numPoints++;
            }
            
            context.write(key, new Text(Arrays.toString(agregPoints).replace("[", "").replace("]", "")+","+String.valueOf(numPoints)));
	}
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text>{


        protected void reduce(IntWritable key, Iterable<Text> value,Context context)
                throws IOException, InterruptedException {
            ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
            
	    //put every points share the same center in to filedslist
            for(Iterator<Text> it =value.iterator();it.hasNext();){
                ArrayList<Double> tempList = Utils.textToArray(it.next());
                filedsList.add(tempList);
            }
            
	    //Similarly, filedsize is 3 in our case
            //int filedSize = filedsList.get(0).size();
            double[] avg = new double[2];
            for(int i=0;i<2;i++){

                double sum = 0;
		//double num = 0;
                int size = filedsList.size();	//size is the number of points share the same certain center
                for(int j=0;j<size;j++){
                    sum += filedsList.get(j).get(i);
		    //num += filedsList.get(j).get(2);
                }
                //avg[i] = sum / num;
		avg[i] = sum / size;
            }
            context.write(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
	    //set key as space
	    //change the output value format from [x,y] to x,y
        }
        
    }
    
    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath) throws IOException, ClassNotFoundException, InterruptedException{
        
        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);

        Job job = new Job(conf, "K_means");
        job.setJarByClass(K_means.class);
        job.setNumReduceTasks(1);	//only use 1 reducer for knowing if the center has changed instantly
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
	//job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(dataPath));
        
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));
        
        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String centerPath = "/Workspace/examples/Seeds";
        String dataPath = "/Workspace/examples/Points";
        String newCenterPath ="/Workspace/examples/kmean";
        String newFilePath ="/Workspace/examples/kmean/part-r-00000";

        int count = 0;
        
	run(centerPath,dataPath,newCenterPath);
	count=1;	//count the number of iterations
	Configuration conf = new Configuration();
	FileSystem hdfs=FileSystem.get(conf);
	System.out.println("The 1st iteration has finished");
	//when the center changes and the number of iterations has not reach 6, start another run
        while((Utils.isFinished(centerPath,newFilePath )==false)&&count<6){

		System.out.println("Distance larger than the threshold");
		Utils.deletePath(centerPath);
		System.out.println("Begin the "+(++count)+"th iteration");
		Path inPath = new Path(newFilePath);
		Path outPath = new Path(centerPath);
            	FileSystem fileSystem = outPath.getFileSystem(conf);
            
            	FSDataOutputStream overWrite = fileSystem.create(outPath,true);
            	overWrite.writeChars("");
            	overWrite.close();
            	FileStatus[] listFiles = fileSystem.listStatus(inPath);
		//copy the new center to the old center path
            	for (int i = 0; i < listFiles.length; i++) {                
                	FSDataOutputStream out = fileSystem.create(outPath);
                	FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                	IOUtils.copyBytes(in, out, 4096, true);
            	}
		Utils.deletePath(newCenterPath);
		run(centerPath,dataPath,newCenterPath);
	}
	if(Utils.isFinished(centerPath,newFilePath )==false){
		System.out.println("Though the centers has changed, we reached 6 iterations. So the algorithm has terminated");
	}
		
    }
    
}
