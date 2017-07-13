package team.CSME.logAnalysis.response;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import team.CSME.logAnalysis.LogItem;

public class ResponseMapper extends Mapper<Object, Text, Text, Text> {
    Text newKey= new Text();
    Text values= new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        
    	LogItem item=new LogItem(value.toString());
    	if(item.getUrl().equals("null")||item.getUrl().equals("-")) return;
    	newKey.set((String.valueOf(item.getStart_hour())+"#"+item.getUrl()));
    	values.set(String.valueOf(1)+"#"+item.getRespond_time());
    	context.write(newKey, values);
    }
}