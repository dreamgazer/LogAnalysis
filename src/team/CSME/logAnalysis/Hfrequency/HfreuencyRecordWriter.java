package team.CSME.logAnalysis.Hfrequency;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class HfreuencyRecordWriter extends RecordWriter<Text, IntWritable> {
	
	protected DataOutputStream out;
	private HashMap<String,ArrayList<ArrayList<Integer>>> URLs ;
	private HashMap<String,Integer[][]> day08_21=new HashMap<String,Integer[][]>();
	private ArrayList<HashMap<String,Integer>> day22=new ArrayList<HashMap<String,Integer>> ();	
	private ArrayList<HashMap<String,Integer>>prediction=new ArrayList<HashMap<String,Integer>> ();
	double[] weights={0,0,0,0,0,0,0,0.0,0.02,0.04,0.05,0.05,0.045,1};
	
	private int fit(int[] X,Integer[] Y,int X0){

		
		
		final WeightedObservedPoints obs = new WeightedObservedPoints();  
		
  	for(int i=0;i<X.length;i++){	
		obs.add( weights[i],X[i],Y[i]);				  
	}

        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(0);
        final double[] coeff = fitter.fit(obs.toList());  
  int y=0;
        for(int i=0;i<coeff.length;i++){
        	y+=coeff[i]*Math.pow(X0, i);
        }
      
        return y;
}
	
	
	 public HfreuencyRecordWriter(DataOutputStream out ) {  
	        this.out = out;  
	        URLs=new HashMap<String,ArrayList<ArrayList<Integer>>> () ;
	        for(int i=0;i<=23;i++){
	    		HashMap<String,Integer>map=new HashMap<String,Integer>();			
	    		day22.add(map);
	    	}
	        for(int i=0;i<=23;i++){
	    		HashMap<String,Integer>map=new HashMap<String,Integer>();   			
	    		prediction.add(map);
	    	}
	    } 
	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		Iterator<Entry<String,ArrayList<ArrayList<Integer>>>> iter = URLs.entrySet().iterator();
    	Entry<String,ArrayList<ArrayList<Integer>>> entry;
    	String Url=null;
    	while (iter.hasNext()) {
    		entry=iter.next();
    		Url=entry.getKey();
    		for(int i=0;i<entry.getValue().size();i++){
    			for(int j=0;j<entry.getValue().get(i).size();j++){
    				if(i<=13)day08_21.get(Url)[j][i]=entry.getValue().get(i).get(j);
    				if(i==14)day22.get(j).put(Url, entry.getValue().get(i).get(j));    			}
    		}
    	}
    	 Iterator<Entry<String,Integer[][]>>day08_21_iter =day08_21.entrySet().iterator();
    	 Entry<String,Integer[][]> day08_21_entry;
    	 while (day08_21_iter.hasNext()) {
    		 day08_21_entry=day08_21_iter.next();
     		Url=day08_21_entry.getKey();
     		for(int i=0;i<day08_21_entry.getValue().length;i++){
     			int prediction_value=0;
     			for(int j=0;j<day08_21_entry.getValue()[i].length;j++){
     				prediction_value+=day08_21_entry.getValue()[i][j]*weights[j];
         		}
     			int[] X={8,9,10,11,12,13,14,15,16,17,18,19,20,21};     			
     		 	prediction_value=fit(X, day08_21_entry.getValue()[i], 22);
     			
     			prediction.get(i).put(Url, prediction_value);
     		
     		}
     	}
    	 double RMSE=0;
    	 for(int i=0;i<=23;i++){
    		 int aveg=0;
    		 Iterator<Entry<String,Integer>> day22_iter=day22.get(i).entrySet().iterator();
    		 Entry<String,Integer> day22_entry;
    		 while(day22_iter.hasNext()){
    			 day22_entry=day22_iter.next();
    		    aveg+=Math.pow((day22_entry.getValue()-prediction.get(i).get(day22_entry.getKey())),2);
    		 }
    		 RMSE+=Math.sqrt(aveg/day22.get(i).size());
    		
    	 }
    	 out.writeChars("RMSE value :"+RMSE/24+"\n");
    	 for(int i=0;i<=23;i++){
    		 Iterator<Entry<String,Integer>> prediction_iter=prediction.get(i).entrySet().iterator();
    		 Entry<String,Integer> prediction_entry;
    		 while(prediction_iter.hasNext()){
    			 prediction_entry= prediction_iter.next();
    			 out.writeChars( prediction_entry.getKey()+" "+i+":00-"+(i+1)+":00 "+prediction_entry.getValue()+"\n");
    		 }
    	 }
    	 
    	 
    	System.out.println(" finished!");
		out.close();
	}

	@Override
	public void write(Text text, IntWritable number) throws IOException, InterruptedException {
		String Url=(((Text)text).toString()).split("#")[0];
    	int hour=Integer.parseInt((((Text)text).toString()).split("#")[1]);
    	int date=Integer.parseInt((((Text)text).toString()).split("#")[2]);
    	int amount=number.get();
    	if(date-8<0||date-8>14) {return;}
    	if(!URLs.containsKey(Url)){
    		ArrayList<ArrayList<Integer>> Days=new ArrayList<ArrayList<Integer>>(15);
    		Integer[][] array08_21=new Integer[24][14];
    		day08_21.put(Url, array08_21);
    		for(int i=0;i<15;i++){
    			ArrayList<Integer> day=new ArrayList<Integer>();
    			
    			for(int j=0;j<=23;j++){
    				day.add(0);
    			}
    			Days.add(day);
    		}
    		URLs.put(Url, Days);
    		URLs.get(Url).get(date-8).set(hour,amount);
    	}
    	else{
    		URLs.get(Url).get(date-8).set(hour,URLs.get(Url).get(date-8).get(hour)+amount);
    	}
	
	}	
	
}
