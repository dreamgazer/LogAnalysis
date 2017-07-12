package team.CSME.logAnalysis.hourfrequency;

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
public class HourFrequencyRecordWriter<K, V> extends RecordWriter<K, V> {  
	private int overall_count=0;
	private String Url;
	Comparator<String> comparator=new Comparator<String>(){
        public int compare(String o1,String o2){
        	for(int i=13;i<=20;i++){
        		if(o1.charAt(i)>o2.charAt(i)){
        			return 1;
        		}
        		else if(o1.charAt(i)<o2.charAt(i)){
        			return -1;
        		}
        	}
        	return 0;
        }
    };
	private HashMap<Integer,Integer> FrequencyMap=new HashMap<Integer,Integer>();
    private static final String utf8 = "UTF-8";  
    static {  
        try {  
            "\n".getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    protected DataOutputStream out;  
    public HourFrequencyRecordWriter(DataOutputStream out, String keyValueSeparator) {  
        this.out = out;  
        try {  
            keyValueSeparator.getBytes(utf8);  
        } catch (UnsupportedEncodingException uee) {  
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");  
        }  
    }  
    public HourFrequencyRecordWriter(DataOutputStream out) {  
        this(out, "\t");  
    }  
    public synchronized void write(K key, V value) throws IOException {  
    	if(!(key instanceof Text)|| !(value instanceof IntWritable)){
    		System.out.println("Wrong K-V!");
    		return;
    	}
    	int hour=Integer.parseInt((((Text)key).toString()).split("#")[1]);
    	//String time=(((Text)key).toString()).split("#")[1];
    	Url=(((Text)key).toString()).split("#")[0];
    	int amount=(( IntWritable)value).get();
    	overall_count+=amount;
    	FrequencyMap.put(hour, amount);
    	
    	
    }  
    public synchronized void close(TaskAttemptContext context) throws IOException {  
    	out.writeChars(Url+":"+overall_count+"\n");
    	
    	for(int i=0;i<=23;i++){
    		if(FrequencyMap.containsKey(i)){
    			out.writeChars(i+":00-"+(i+1)+":00 ");
        		out.writeChars(FrequencyMap.get(i)+"\n");
    		}
    	}
        out.close();  
    }  
}  
