package team.CSME.logAnalysis.IP;

import java.io.DataOutputStream;
import java.io.IOException;  
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
public class IPRecordWriter<K, V> extends RecordWriter<K, V> {  
	private int overall_count=0;
	private String IP;
	private HashMap<Integer,Integer> IPmap=new HashMap<Integer,Integer>();
    private static final String utf8 = "UTF-8";  
    static {  
        try {  
            "\n".getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    protected DataOutputStream out;  
    public IPRecordWriter(DataOutputStream out, String keyValueSeparator) {  
        this.out = out;  
        try {  
            keyValueSeparator.getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    public IPRecordWriter(DataOutputStream out) {  
        this(out, "\t");  
    }  
    public synchronized void write(K key, V value) throws IOException {  
    	if(!(key instanceof Text)|| !(value instanceof IntWritable)){
    		System.out.println("Wrong K-V!");
    		return;
    	}
    	int start_hour=Integer.parseInt((((Text)key).toString()).split("#")[0]);
    	IP=(((Text)key).toString()).split("#")[1];
    	int amount=(( IntWritable)value).get();
    	overall_count+=amount;
    	if(!IPmap.containsKey(start_hour)){
    		IPmap.put(start_hour, amount);
    	}
    	else{
    		IPmap.put(start_hour, amount+IPmap.get(start_hour));
    	}
    }  
    public synchronized void close(TaskAttemptContext context) throws IOException {  
    	out.writeChars(IP+":"+overall_count+"\n");
    	for(int i=0;i<=23;i++){
    		out.writeChars(i+":00-"+(i+1)+":00 ");
    		out.writeChars(IPmap.get(i)+"\n");
    	}
        out.close();  
    }  
}  
