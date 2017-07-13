package team.CSME.logAnalysis.hourfrequency;

import java.io.DataOutputStream;  
import java.io.IOException;  
import java.util.HashMap;  
import java.util.Iterator;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.GzipCodec;  
import org.apache.hadoop.mapreduce.OutputCommitter;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.ReflectionUtils;

import team.CSME.logAnalysis.status.StatusRecordWriter;


public class HourFrequencyOutputFormat extends FileOutputFormat<Text, IntWritable>{

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		 Configuration conf = job.getConfiguration();  
	        boolean isCompressed = getCompressOutput(job);  
	        CompressionCodec codec = null;  
	        String extension = "";  
	        if (isCompressed) {  
	            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);  
	            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);  
	            extension = codec.getDefaultExtension();  
	        }  
	        Path file = getDefaultWorkFile(job, extension);  
	        FileSystem fs = file.getFileSystem(conf);  
	 
	        if (!isCompressed) {  
	            FSDataOutputStream fileOut = fs.create(file, false);  
	            return new HourFrequencyRecordWriter(fileOut);  
	        } else {  
	            FSDataOutputStream fileOut = fs.create(file, false);  
	            return new HourFrequencyRecordWriter(new DataOutputStream( codec.createOutputStream(fileOut)));  
	        }  
	}


	
}