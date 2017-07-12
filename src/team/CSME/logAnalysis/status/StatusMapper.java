package team.CSME.logAnalysis.status;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import team.CSME.logAnalysis.LogItem;

public class StatusMapper extends Mapper<Object, Text, Text, IntWritable> {
    private  IntWritable  one=new IntWritable(1);
    Text newKey= new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        
    	LogItem item=new LogItem(value.toString());
    	newKey.set((String.valueOf(item.getStart_hour())+"#"+item.getStatus()));
    	context.write(newKey, one);
    }
}