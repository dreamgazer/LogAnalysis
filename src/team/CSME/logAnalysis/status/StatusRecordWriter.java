package team.CSME.logAnalysis.status;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class StatusRecordWriter extends RecordWriter<Text, IntWritable> {
	
	protected DataOutputStream out;
	
	class Status{
		private int num_404;
    	private int num_200;
    	private int num_400;
    	public Status(){
    		num_404=0;
    		num_200=0;
    		num_400=0;
    	}
    	
    	public void increase(int key,int amount){
    		switch(key){
    			case 200:{num_200+=amount;break;}
    			case 400:{num_400+=amount;break;}
    			case 404:{num_404+=amount;break;}
    			default:System.out.println("unfound status:"+key);
    		}
    	}

    	public int get_200_num(){
    		return num_200;
    	}
   
    	public int get_400_num(){
    		return num_400;
    	}
    	
    	public int get_404_num(){
    		return num_404;
    	}   	
	}
	
	Status overallStatus;
	HashMap<Integer,Status> statusMap;
	 public  StatusRecordWriter(DataOutputStream out ) {  
	        this.out = out;  
	        overallStatus= new Status();
	        statusMap=new HashMap<Integer,Status>();
	    } 
	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		out.writeChars("200:");
		out.writeChars(overallStatus.get_200_num()+"\n");
		out.writeChars("404:");
		out.writeChars(overallStatus.get_404_num()+"\n");
		out.writeChars("400:");
		out.writeChars(overallStatus.get_400_num()+"\n");
		for(int i=0;i<=23;i++){
			if(statusMap.containsKey(i)){					
				out.writeChars(i+":00-"+(i+1)+":00");
				out.writeChars(" 200:"+statusMap.get(i).get_200_num());
				out.writeChars(" 404:"+statusMap.get(i).get_404_num());
				out.writeChars(" 400:"+statusMap.get(i).get_400_num());
				out.writeChars("\n");
			}
			
		}
		out.close();
	}

	@Override
	public void write(Text text, IntWritable amount) throws IOException, InterruptedException {
		String[] args=text.toString().split("#");
		if(statusMap.containsKey(Integer.parseInt(args[0]))){
			statusMap.get(Integer.parseInt(args[0])).increase(Integer.parseInt(args[1]),amount.get());	
			overallStatus.increase(Integer.parseInt(args[1]),amount.get());
		}
		else{
			statusMap.put(Integer.parseInt(args[0]), new Status());
			statusMap.get(Integer.parseInt(args[0])).increase(Integer.parseInt(args[1]),amount.get());	
			overallStatus.increase(Integer.parseInt(args[1]),amount.get());
		}
		
	}	
}
