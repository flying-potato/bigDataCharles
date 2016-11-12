
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistrictPeopleMapper
	extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		// line = line.toLowerCase();
		
		String SPEC_DATE = "08/11/2015"; //pass DATE as an argument

		if(line.contains(SPEC_DATE)){
			String[] split = line.split(",");
			String station = split[3];
			String entries = split[9];
			String exits = split[10];
			String mix = entries+";"+exits;
			// int entries = Integer.parseInt(split[9]);
			// int exits = Integer.parseInt(split[10]);
			context.write(new Text(station), new Text(mix));
			
		}

	}
}
	


