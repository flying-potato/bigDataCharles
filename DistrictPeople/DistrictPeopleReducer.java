
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistrictPeopleReducer
	extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		int min_enter = Integer.MAX_VALUE;
		int max_enter = Integer.MIN_VALUE;
		int diff_enter;

		int min_exit = Integer.MAX_VALUE;
		int max_exit = Integer.MIN_VALUE;
		int diff_exit;

		for (Text value : values){
			String svalue = value.toString();
			String[] value_split = svalue.split(";");

			int enter = Integer.parseInt(value_split[0]);
			max_enter = (enter>max_enter?enter:max_enter); 
			min_enter = (enter<min_enter?enter:min_enter); 
			
			
			// int exit  = Integer.parseInt(value_split[1]);
			// max_exit = (exit>max_exit?exit:max_exit);
			// min_exit = (exit<min_exit?exit:min_exit)
		}
		diff_enter = max_enter - min_enter;
		String out_diff_enter = String.valueOf(diff_enter);
		// diff_exit = max_exit - min_exit;

		context.write(key, new Text(out_diff_enter));
	}
}
