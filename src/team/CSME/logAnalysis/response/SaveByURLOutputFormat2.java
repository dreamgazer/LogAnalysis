package team.CSME.logAnalysis.response;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class SaveByURLOutputFormat2 extends ResponseMultipleOutputFormat<Text, Text>{
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {
		// TODO Auto-generated method stub
		return key.toString().split("#")[1].replaceAll("/", "-").replaceFirst("-", "");
	}
}
