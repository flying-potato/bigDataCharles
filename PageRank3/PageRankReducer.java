import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer 
	extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		double finalPR = 0;
		String des="";
		String result;
		for (Text value: values) {
			String eachvalue  = values.toString();
			try{
				String[] sp = eachvalue.split(" ");
				double eachPR =  Double.parseDouble(sp[1]);
				finalPR += eachPR;
			}catch (Exception e){
				des = eachvalue; 
			}
		}
		result = des + " " + Double.toString(finalPR);
		context.write(key, new Text(result));
	}
}
