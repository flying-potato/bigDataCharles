import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddressPeopleMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		String line = value.toString();
		// line = line.toLowerCase();
		String[] sp = line.split(",");
		String timeadd =  sp[0]+","+sp[1];
		String peoplestr = sp[2];
		int people = Integer.parseInt(peoplestr);
		context.write(new Text(timeadd), new IntWritable(people));
	}
}
	


