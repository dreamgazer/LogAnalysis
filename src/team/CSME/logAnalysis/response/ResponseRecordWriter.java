package team.CSME.logAnalysis.response;

import java.io.DataOutputStream;
import java.io.IOException;  
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
public class ResponseRecordWriter<K, V> extends RecordWriter<K, V> {  
	private int overall_count=0;
	private int overall_time=0;
	private String Url;

	private HashMap<Integer,Double> responseMap=new HashMap<Integer,Double>();
    private static final String utf8 = "UTF-8";  
    static {  
        try {  
            "\n".getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    protected DataOutputStream out;  
    public ResponseRecordWriter(DataOutputStream out, String keyValueSeparator) {  
        this.out = out;  
        try {  
            keyValueSeparator.getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    public ResponseRecordWriter(DataOutputStream out) {  
        this(out, "\t");  
    }  
    public synchronized void write(K key, V value) throws IOException {  
    	if(!(key instanceof Text)|| !(value instanceof Text)){
    		System.out.println("Wrong K-V!");
    		return;
    	}
    	int start_hour=Integer.parseInt((((Text)key).toString()).split("#")[0]);
    	Url=(((Text)key).toString()).split("#")[1];
    	int count=Integer.parseInt((((Text)value).toString()).split("#")[0]);
    	int amount=Integer.parseInt((((Text)value).toString()).split("#")[1]);
    	double average=(double)amount/(double)count;
    	overall_count+=count;
    	overall_time+=amount;
    	responseMap.put(start_hour, average);
    	
   
    }  
    public synchronized void close(TaskAttemptContext context) throws IOException {  
    	out.writeChars("Average Responed Time \n");
    	out.writeChars(Url+":"+(double)overall_time/(double)overall_count+"\n");
  	
    	for(int i=0;i<=23;i++){
    		if(responseMap.containsKey(i)){
    	    		out.writeChars(i+":00-"+(i+1)+":00 ");
    	    		out.writeChars(responseMap.get(i)+"\n"); 	
    		}
    	}
        out.close();  
    }  
}  
