package wordCount;
import java.io.*;
import java.util.*;

public class NonMRWordCount {
	public static void findString(String[] searchStr){
		HashMap<String, Integer> hm = new HashMap<String, Integer>();
		for (String x: searchStr){
			hm.put(x.toLowerCase(), 0);
		}
		FileInputStream in = null;
		FileOutputStream out = null;
		
		try{
			in = new FileInputStream("testing.txt");
		    InputStreamReader inr = new InputStreamReader(in);
		    BufferedReader br = new BufferedReader(inr);
		    
			BufferedWriter writer = null;
			
		    try{
			    String line = "";
			    while ((line = br.readLine()) != null) {
			        String[] split = line.split("#|,|:|/|-| "); // split one line into words String array
			        Set<String> hs = new HashSet<String>(Arrays.asList(split));
			        //System.out.print(hs.toString());
			        
			        Iterator<String> iter = hs.iterator();	        
			        while(iter.hasNext()){
			        	String x = iter.next();
			        	if(!x.equals(" ") &&!x.isEmpty()){
			        		x = x.toLowerCase().trim();
			        		if(hm.containsKey(x)){
			        			Integer newvalue = hm.get(x)+1;
			        			hm.put(x, newvalue);
			        		}
			        	}
			        }
			        line = br.readLine();
			    }
		    }catch (IOException e){
		    	System.out.println(e.getMessage());
		    }
		    
		    System.out.print(hm.toString());
		    try{
		    	out = new FileOutputStream("output.txt");
		    	PrintStream outp = new PrintStream(out);
		    	outp.print(hm.toString());
		    }catch (IOException e){}
		    
		}catch (FileNotFoundException e){
			System.out.println(e.getMessage());
		}
	}
	public static void main(String[] args){
		String[] searchStr = {"dec", "java", "hackathon", "Chicago"};
		findString(searchStr);
		
	}
}
