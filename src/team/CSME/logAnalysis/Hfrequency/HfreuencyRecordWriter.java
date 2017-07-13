package team.CSME.logAnalysis.Hfrequency;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class HfreuencyRecordWriter extends RecordWriter<Text, IntWritable> {
	
	protected DataOutputStream out;
	private HashMap<String,ArrayList<ArrayList<Integer>>> URLs ;
	
	
	
	 public HfreuencyRecordWriter(DataOutputStream out ) {  
	        this.out = out;  
	        URLs=new HashMap<String,ArrayList<ArrayList<Integer>>> () ;
	    } 
	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		Iterator<Entry<String,ArrayList<ArrayList<Integer>>>> iter = URLs.entrySet().iterator();
    	Entry<String,ArrayList<ArrayList<Integer>>> entry;
    	while (iter.hasNext()) {
    		entry=iter.next();
    		out.writeChars("Url "+entry.getKey()+"\n ");
    		System.out.println("Url "+entry.getKey()+"\n ");
    		for(int i=0;i<entry.getValue().size();i++){
    			out.writeChars("Date "+(i+8)+"\n");
    			for(int j=0;j<entry.getValue().get(i).size();j++){
    				out.writeChars("Hour "+j+" "+entry.getValue().get(i).get(j)+"\n");
    				//System.out.println("Hour "+j+entry.getValue().get(i).get(j)+"\n");
    			}
    		}
    	}
    	System.out.println(" finished!");
		out.close();
	}

	@Override
	public void write(Text text, IntWritable number) throws IOException, InterruptedException {
		System.out.println(text+"---- "+number+"\n ");
		String Url=(((Text)text).toString()).split("#")[0];
    	int hour=Integer.parseInt((((Text)text).toString()).split("#")[1]);
    	int date=Integer.parseInt((((Text)text).toString()).split("#")[2]);
    	int amount=number.get();
    	if(date-8<0||date-8>14) {return;}
    	if(!URLs.containsKey(Url)){
    		ArrayList<ArrayList<Integer>> Days=new ArrayList<ArrayList<Integer>>(15);
    		for(int i=0;i<15;i++){
    			ArrayList<Integer> day=new ArrayList<Integer>();
    			
    			for(int j=0;j<=23;j++){
    				day.add(0);
    			}
    			Days.add(day);
    		}
    		//System.out.println(Url);
    		URLs.put(Url, Days);
    		URLs.get(Url).get(date-8).set(hour,amount);
    	}
    	else{
    		//System.out.println(Url);
    		URLs.get(Url).get(date-8).set(hour,URLs.get(Url).get(date-8).get(hour)+amount);
    	}
	
	}	
	
}
