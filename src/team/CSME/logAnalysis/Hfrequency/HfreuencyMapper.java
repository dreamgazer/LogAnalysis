package team.CSME.logAnalysis.Hfrequency;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import team.CSME.logAnalysis.LogItem;

public class HfreuencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private  IntWritable  one=new IntWritable(1);
    Text newKey= new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        
    	if(value.toString().split(" ").length<4) return;
    	LogItem item=new LogItem(value.toString());
    	if(item.getUrl().equals("null")||item.getUrl().equals("-")||item.getUrl().equals("172.22.49.26")) return;
    	newKey.set((String.valueOf(item.getUrl())+"#"+item.getStart_hour()+"#"+item.getDate()));
    	context.write(newKey, one);
    }
}