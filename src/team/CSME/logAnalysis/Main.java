package team.CSME.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import team.CSME.logAnalysis.IP.*;
import team.CSME.logAnalysis.frequency.*;
import team.CSME.logAnalysis.response.*;
import team.CSME.logAnalysis.status.*;

public class Main {
	private static Job job;
	private static Configuration conf ;
	private static String inputFile;
	
	@SuppressWarnings("deprecation")
	public static void status_analyze(String filename) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		 System.out.println("--------------------------------------status_analyze begins--------------------------------------");
		 job = new Job(conf, "status_analyze");
	        job.setJarByClass(Main.class);
	        job.setMapperClass(StatusMapper.class);
	        job.setCombinerClass(StatusReducer.class);
	        job.setReducerClass(StatusReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        job.setOutputFormatClass(StatusOutputFormat.class);
	        FileInputFormat.addInputPath(job, new Path(inputFile));
	        FileOutputFormat.setOutputPath(job, new Path(filename));
	        job.waitForCompletion(true);
	        System.out.println("--------------------------------------status_analyze finished--------------------------------------");
	}
	
	@SuppressWarnings("deprecation")
	public static void IP_analyze(String filename) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{	 
		System.out.println("--------------------------------------IP_analyze begins--------------------------------------");
		job = new Job(conf, "IP_analyze");
	        job.setJarByClass(Main.class);
	        job.setMapperClass(IPMapper.class);
	        job.setCombinerClass(IPReducer.class);
	        job.setReducerClass(IPReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        job.setOutputFormatClass(SaveByIPOutputFormat.class);
	        FileInputFormat.addInputPath(job, new Path(inputFile));
	        FileOutputFormat.setOutputPath(job, new Path(filename));
	        job.waitForCompletion(true);
	        System.out.println("IP_analyze finished");
	}
	
	@SuppressWarnings("deprecation")
	public static void frequency_analyze(String filename) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		System.out.println("--------------------------------------ffrequency_analyze begins--------------------------------------f");
		job = new Job(conf, "frequency_analyze");
        job.setJarByClass(Main.class);
        job.setMapperClass(FrequencyMapper.class);
        job.setCombinerClass(FrequencyReducer.class);
        job.setReducerClass(FrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SaveByURLOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(filename));
        job.waitForCompletion(true);
        System.out.println("--------------------------------------frequency_analyze finished--------------------------------------");
	}
	
	@SuppressWarnings("deprecation")
	public static void response_analyze(String filename) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		System.out.println("--------------------------------------response_analyze begins--------------------------------------");
		job = new Job(conf, "response_analyze");
	        job.setJarByClass(Main.class);
	        job.setMapperClass(ResponseMapper.class);
	        job.setCombinerClass(ResponseReducer.class);
	        job.setReducerClass(ResponseReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        job.setOutputFormatClass(SaveByURLOutputFormat2.class);
	        FileInputFormat.addInputPath(job, new Path(inputFile));
	        FileOutputFormat.setOutputPath(job, new Path(filename));
	        job.waitForCompletion(true);
	        System.out.println("--------------------------------------response_analyze finished--------------------------------------");
	}
	
	
	 public static void main(String[] args) throws Exception {		 
		    conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 5) {
	            System.err.println("Usage: logAnalyze <in> <out1><out2><out3><out4>");
	            System.exit(2);
	        }
	        inputFile=otherArgs[0];
	               	       
	        status_analyze(otherArgs[1]);
	        	        
	        IP_analyze(otherArgs[2]);
	        
	        frequency_analyze(otherArgs[3]);
	        
	        response_analyze(otherArgs[4]);	     
	        
	        System.out.println("--------------------------------------Task1-Task4 finished--------------------------------------");
	    }
}
