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




public class GetHourFrequencyFile {

    public static void main(String[] args) throws Exception {
    		HashMap<String,ArrayList<ArrayList<Integer>>> URLs=new HashMap<String,ArrayList<ArrayList<Integer>>> () ;   	
    		for(int i=8;i<=22;i++){
    			String filename="hour_frequency_file/"+i+".txt ";
    			FileReader fr = new FileReader(filename);
    			BufferedReader bufferedReader = new BufferedReader(fr);
                String lineTxt = null;
                String Date,Url="null ",Hour;
                int date=0,hour=0,amount=0;
                while((lineTxt = bufferedReader.readLine()) != null){
                	lineTxt=URLEncoder.encode(lineTxt, "UTF-8");
                	lineTxt=lineTxt.replaceAll("\\+", " ");
                	 lineTxt=lineTxt.replaceAll("\\%2F", "/");
                	if(i==13||i==14||i==18||i==19||i==20){           		
                		lineTxt=lineTxt.replaceAll("\\%23", "#");
                		
                		if(lineTxt.split(" ").length<2) continue;
                		Url=(lineTxt.split(" ")[0]).split("#")[0];
                		hour=Integer.parseInt((lineTxt.split(" ")[0]).split("#")[1]);
                    	date=Integer.parseInt((lineTxt.split(" ")[0]).split("#")[2]);
                    	amount=Integer.parseInt(lineTxt.split(" ")[1]);
                    	if(date-8<0||date-8>14) {continue;}
                    	if(!URLs.containsKey(Url)){
                    		
                    		ArrayList<ArrayList<Integer>> Days=new ArrayList<ArrayList<Integer>>(15);
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
                    		//System.out.println(i+".txt "+lineTxt);
                    	//	System.out.println("url:"+Url+"amount"+amount+"date:"+date);
                    		URLs.get(Url).get(date-8).set(hour,URLs.get(Url).get(date-8).get(hour)+amount);
                    	}
                	}
                	else{               		
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
                }  			
    		}		
    		String path="day8-day20.txt";
            File file=new File(path);
            if(!file.exists())
                file.createNewFile();
            FileOutputStream out=new FileOutputStream(file,false); //如果追加方式用true        
            StringBuffer sb=new StringBuffer();
            Iterator<Entry<String,ArrayList<ArrayList<Integer>>>> iter = URLs.entrySet().iterator();
        	Entry<String,ArrayList<ArrayList<Integer>>> entry;
        	  String Date,Url="null ",Hour;
              int date=0,hour=0,amount=0;
        	while (iter.hasNext()) {
        		entry=iter.next();
        		Url=entry.getKey();
        		// sb.append("Url "+entry.getKey()+"\n");
        		for(int i=0;i<entry.getValue().size();i++){
        			date=(i+8);
        			// sb.append("Date "+(i+8)+"\n");
        			for(int j=0;j<entry.getValue().get(i).size();j++){
        				sb.append(Url+" "+date+" "+j+" "+entry.getValue().get(i).get(j)+"\n");
        				 //sb.append("Hour "+j+" "+entry.getValue().get(i).get(j)+"\n");
        			}
        		}
        	}
        	out.write(sb.toString().getBytes("utf-8"));
            out.close();
    }
}