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

import team.CSME.logAnalysis.hourfrequency.*;

public class HourFrequencyMain {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "HourFrequency");
        job.setJarByClass(HourFrequencyMain.class);
        job.setMapperClass(HourFrequencyMapper.class);
        job.setCombinerClass(HourFrequencyReducer.class);
        job.setReducerClass(HourFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SaveByURLOutputFormat3.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
}
