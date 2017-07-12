package team.CSME.logAnalysis;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogItem {
			private String IP;
			private String time;
			private int date;
			private int start_hour;
			private String method;
			private String url;
			private String status;
			private String respond_length;
			private String respond_time;
			
			public LogItem(String IP, int date, int start_hour,String method,String url,String status,String respond_length,String respond_time){
				this.IP=IP;
				this.time=time;
				this.date=date;
				this.start_hour=start_hour;
				this.method=method;
				this.url=url;
				this.status=status;
				this.respond_length=respond_length;
				this.respond_time=respond_time;
			}
			
			
			public LogItem(String Log_str){
				Pattern pattern;
				Matcher matcher ;
				String[] strs=Log_str.split(" ");
				this.method=strs[strs.length-4];
				this.status=strs[strs.length-3];
				this.respond_length=strs[strs.length-2];
				this.respond_time=strs[strs.length-1];
				this.IP=strs[0];
				
				
				pattern = Pattern.compile("\".*\"");
				matcher = pattern.matcher(Log_str);
					String str;
					if(matcher.find()){
						str= matcher.group();
						this.url=str.split(" ")[1];
					}
					else{
						this.url=strs[strs.length-5];
					}			

				pattern = Pattern.compile("\\[.*\\]");
				matcher = pattern.matcher(Log_str);
				if(matcher.find()){
					str= matcher.group();
					this.time=str.substring(1, 21);
				    this.date=Integer.parseInt(str.substring(1, 3));
					this.start_hour=Integer.parseInt(str.substring(13, 15));
				}
				else{
					System.out.println(Log_str);
				}
			

				
			}
			
			
			public String getIP() {
				return IP;
			}
			public void setIP(String iP) {
				IP = iP;
			}
			public int getDate() {
				return date;
			}
			public void setDate(int date) {
				this.date = date;
			}
			public int getStart_hour() {
				return start_hour;
			}
			public void setStart_hour(int start_hour) {
				this.start_hour = start_hour;
			}
			public String getMethod() {
				return method;
			}
			public void setMethod(String method) {
				this.method = method;
			}
			public String getUrl() {
				return url;
			}
			public void setUrl(String url) {
				this.url = url;
			}
			public String getStatus() {
				return status;
			}
			public void setStatus(String status) {
				this.status = status;
			}
			public String getRespond_length() {
				return respond_length;
			}
			public void setRespond_length(String respond_length) {
				this.respond_length = respond_length;
			}
			public String getRespond_time() {
				return respond_time;
			}
			public void setRespond_time(String respond_time) {
				this.respond_time = respond_time;
			}


			public String getTime() {
				return time;
			}


			public void setTime(String time) {
				this.time = time;
			}
}
