package team.CSME.logAnalysis.response;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResponseReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0,count=0;
       
        for (Text val : values) {
        	count+= Integer.parseInt((val.toString().split("#"))[0]);
            sum +=  Integer.parseInt((val.toString().split("#"))[1]);
        }
        result.set(String.valueOf(count)+"#"+String.valueOf(sum));
        context.write(key, result);
      
    }
}
