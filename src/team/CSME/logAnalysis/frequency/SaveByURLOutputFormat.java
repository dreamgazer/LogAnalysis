package team.CSME.logAnalysis.frequency;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class SaveByURLOutputFormat extends FrequencyMultipleOutputFormat<Text, IntWritable>{
	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value, Configuration conf) {
		// TODO Auto-generated method stub
		return key.toString().split("#")[0].replaceAll("/", "-").replaceFirst("-", "");
	}
}
