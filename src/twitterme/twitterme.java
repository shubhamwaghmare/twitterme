

import java.io.*;
import java.math.BigDecimal; //for getting float result upto 2 decimals
import java.util.*;

import java.nio.*;
import org.apache.commons.lang.time.StopWatch;  // package for timer function
import org.apache.hadoop.fs.Path;        // for accessing filesystem & delete intermediate output files
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;      //for map-reduce classes
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;


public class twitterme
{
	// 1st Map function to split the input file in key and value
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text ,IntWritable>
	{
		
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{
		String[] line = value.toString().split("\t");

		String followee = line[0];
		String follower=line[1];
		output.collect(new Text(follower.toString()), new IntWritable(Integer.parseInt(followee)));
		
	}
	}
	
	// 1st Reducer to count the number of outlinks from each follower
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text , IntWritable>
	{
         
         
		@Override
		public void reduce(Text key,Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
		{
		   String B;
			int count=0;
		
     	ArrayList<String> a1 = new ArrayList<String>();
		    
			
			while(values.hasNext())
			{
		
				count+=1;
		
				
		a1.add(values.next().toString());   //caching values into arraylist
		
			}
			

	 for(String itr:a1)
			{
		 B=key.toString()+":"+itr;
		 
		 
        output.collect(new Text(B),new IntWritable(count)); //gives follower & followee as key and number of outlinks as value
	    }
         
		}
	}
	
	// 2nd map function to split the intermediate output file say "a" created above
	//in followee as key and outlinks as values
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text , IntWritable>
	{
		
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{
		String[] line = value.toString().split("\t");


		String s2 = line[0];
		
		String[] v = s2.split(":");
		
		String f1 = v[1];
		int f2 =Integer.parseInt(v[2]);
		output.collect(new Text(f1.toString()), new IntWritable(f2));
		
	}
	}
	
	// 2nd Reducer  to give total number of followers of a followee(count), 
	// and sum of outlinks of all followers for each followee
	// and a result as rank value of each user
	
	public static class Reduce1 extends MapReduceBase implements Reducer<Text, IntWritable,Text,FloatWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,FloatWritable> output, Reporter reporter) throws IOException 
		{
			float count=0;
			float sum=0;
			float result;
		
			while(values.hasNext())
			{ 
			    sum=sum+values.next().get();
				count+=1;
		       
					
			}
		
		result=count-sum/count;
		
		BigDecimal bd=new BigDecimal(Float.toString(result));  // to roundup result to 2 decimal places
			bd=bd.setScale(2,BigDecimal.ROUND_UP);
			output.collect(key,new FloatWritable(bd.floatValue()));
		}
		}
	
	// 3rd mapper to split the 2nd intermediate output file "b" into
	//result as key
	//followee as value
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable,Text>
	{
		
	public void map(LongWritable key, Text value, OutputCollector<FloatWritable,Text> output, Reporter reporter) throws IOException
	{
		String[] line = value.toString().split("\t");


		String s2 = line[0];
		float f2 =Float.parseFloat(line[1]);
		output.collect( new FloatWritable(f2),new Text(s2));
		
	}
	}
	
	// 3rd reducer to assign a rank to each user 
	
	public static class Reduce2 extends MapReduceBase implements Reducer<FloatWritable,Text,IntWritable,Text>
	{ 
	 int rank=0;
		public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException 
		{
			while(values.hasNext())
			{
		       
					rank=rank+1;
			output.collect(new IntWritable(rank),values.next());
			}
		}
		}

		// map reduce sorts values in ascending order by default
		// following class is to arrange the rank values i.e "result" in desc order
public static class IntComparator extends WritableComparator
	{
		public IntComparator()
		{
			super(IntWritable.class);
		}
		public int compare(byte[] b1,int s1,int l1,byte[] b2, int s2, int l2)
		{
			Integer v1 = ByteBuffer.wrap(b1,s1,l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2,s2,l2).getInt();
			return v1.compareTo(v2)*(-1);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		String dir = "hdfs://localhost:9000/data/small/";
		
		Configuration c = new Configuration();
		c.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(c);
		
		
		
		StopWatch timer = new StopWatch();
		timer.start();
		
		// 1st Job
		JobConf conf = new JobConf (twitterme.class);
		conf.setJobName("twitterme");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
		
		//2nd Job
		JobConf conf1 = new JobConf (twitterme.class);
		conf1.setJobName("job2");
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);
		
		conf1.setMapperClass(Map1.class);
		
		conf1.setReducerClass(Reduce1.class);
		
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf1, new Path(args[2]));
		
		JobClient.runJob(conf1);

		
		// 3rd job
		JobConf conf2 = new JobConf (twitterme.class);
		conf2.setJobName("job3");
	    conf2.setOutputKeyComparatorClass(IntComparator.class);
		conf2.setOutputKeyClass(FloatWritable.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapperClass(Map2.class);
		
		conf2.setReducerClass(Reduce2.class);
		
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf2, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[3]));
		
		JobClient.runJob(conf2);

		timer.stop();
		System.out.println("Elapsed:"+timer.toString());
		
		// to delete intermediate output files
		fs.delete(new Path(dir + "a"), true);
		fs.delete(new Path(dir + "b"), true);
	}
	
	
}