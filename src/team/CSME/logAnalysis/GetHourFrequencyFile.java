package team.CSME.logAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Map.Entry;


import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;  

public class GetHourFrequencyFile {
	static double[] weights={0,0,0,0,0,0,0,0.0,0.02,0.04,0.05,0.05,0.045,1};
	
	private static int fit(int[] X,Integer[] Y,int X0){

			final WeightedObservedPoints obs = new WeightedObservedPoints();  
			
	  	for(int i=0;i<X.length;i++){	
			obs.add( weights[i],X[i],Y[i]);				  
		}
	   
	  	
	        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(0);  
	  
	        // Retrieve fitted parameters (coefficients of the polynomial function).  
	        final double[] coeff = fitter.fit(obs.toList());  
	        int y=0;
	        for(int i=0;i<coeff.length;i++){
	        	y+=coeff[i]*Math.pow(X0, i);
	        }
	        
	        return y;
	}

    public static void main(String[] args) throws Exception {
    		HashMap<String,ArrayList<ArrayList<Integer>>> URLs=new HashMap<String,ArrayList<ArrayList<Integer>>> () ;   	

    		HashMap<String,Integer[][]> day08_21=new HashMap<String,Integer[][]>();
    		ArrayList<HashMap<String,Integer>> day22=new ArrayList<HashMap<String,Integer>> ();
    		for(int i=0;i<=23;i++){
    			HashMap<String,Integer>map=new HashMap<String,Integer>();			
    			day22.add(map);
    		}
    		ArrayList<HashMap<String,Integer>>prediction=new ArrayList<HashMap<String,Integer>> ();
   		
    		for(int i=0;i<=23;i++){
    			HashMap<String,Integer>map=new HashMap<String,Integer>();   			
    			prediction.add(map);
    		}
    		
    		Integer[] url_num=new Integer[24];
    			String filename="all_in.txt";
    			FileReader fr = new FileReader(filename);
    			BufferedReader bufferedReader = new BufferedReader(fr);
                String lineTxt = null;
                String Date,Url="null ",Hour;
                int date=0,hour=0,amount=0;
                while((lineTxt = bufferedReader.readLine()) != null){
                	lineTxt=URLEncoder.encode(lineTxt, "UTF-8");
                	lineTxt=lineTxt.replaceAll("\\+", " ");
                	 lineTxt=lineTxt.replaceAll("\\%2F", "/");
      
                		String[] strs=lineTxt.split(" ");                		
                		if(strs[0].equals("Url")) Url=strs[1];
                		if(strs[0].equals("Date")) {date=Integer.parseInt(strs[1]);continue;}
                	    if (strs.length>1){ if(strs[1].equals("Date")) { date=Integer.parseInt(strs[2]);}}
                	//	System.out.println(strs[0]);
                		if(strs[0].equals("Hour")) {
                		
                			hour=Integer.parseInt(strs[1]);
                			amount=Integer.parseInt(strs[2]);
                			if(!URLs.containsKey(Url)){
                	    		ArrayList<ArrayList<Integer>> Days=new ArrayList<ArrayList<Integer>>(15);
                	    		Integer[][] array08_21=new Integer[24][14];
                	    		day08_21.put(Url, array08_21);
                	    
                	    		for(int k=0;k<15;k++){
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
    		String path="all_out.txt";
            File file=new File(path);
            if(!file.exists())
                file.createNewFile();
            FileOutputStream out=new FileOutputStream(file,false); //如果追加方式用true        
            StringBuffer sb=new StringBuffer();
            Iterator<Entry<String,ArrayList<ArrayList<Integer>>>> iter = URLs.entrySet().iterator();
        	Entry<String,ArrayList<ArrayList<Integer>>> entry;
        	while (iter.hasNext()) {
        		entry=iter.next();
        		Url=entry.getKey();
        		for(int i=0;i<entry.getValue().size();i++){
        			date=(i+8);
        			for(int j=0;j<entry.getValue().get(i).size();j++){
        				sb.append(Url+" "+date+" "+j+" "+entry.getValue().get(i).get(j)+"\n");
        				if(i<=13)day08_21.get(Url)[j][i]=entry.getValue().get(i).get(j);
        				if(i==14)day22.get(j).put(Url, entry.getValue().get(i).get(j));
        				
        			}
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
         				prediction_value=day08_21_entry.getValue()[i][j];
             		}
         			double[] X0=new double[1];
         			int[] X={8,9,10,11,12,13,14,15,16,17,18,19,20,21};
         			double[] Y=new double[14];
         			for(int j=0;j<=13;j++){
         				Y[j]=day08_21_entry.getValue()[i][j];
         			}
         			X0[0]=21;
         			
         			//double[] pred=Lag_method(X ,Y,X0);
         			//prediction_value=fit(X, day08_21_entry.getValue()[i], 22);
         			prediction.get(i).put(Url, prediction_value);
         		}
         	}
        	 double RMSE=0;
        	 for(int i=0;i<=23;i++){
        		 int aveg=0;
        		 Iterator<Entry<String,Integer>> day22_iter=day22.get(i).entrySet().iterator();
        		 Entry<String,Integer> day22_entry;
        		 int n=day22.get(i).size();
        		 while(day22_iter.hasNext()){
        			 day22_entry=day22_iter.next();
        			 if(day22_entry.getValue()==0) n--;
        		    aveg+=Math.pow((day22_entry.getValue()-prediction.get(i).get(day22_entry.getKey())),2);
        		   // System.out.println( day22_entry.getValue()+"---"+prediction.get(i).get(day22_entry.getKey()));
        		 }
        		 RMSE+=Math.sqrt(aveg/n);
        		
        	 }
        	 System.out.println(RMSE/24);
        	out.write(sb.toString().getBytes("utf-8"));
            out.close();
    }
}