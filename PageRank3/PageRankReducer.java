import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException {
		double finalPR = 0;
		String des="";
		String result;
		while (values.hasNext()) {
			String eachvalue  = values.next().toString();
			try{
				String[] sp = eachvalue.split(" ");
				double eachPR =  Double.parseDouble(sp[1]);
				finalPR += eachPR;
			}catch (Exception e){
				des = eachvalue; 
			}
		}
		result = des + " " + Double.toString(finalPR);
		output.collect(key, new Text(result));
	}
}
