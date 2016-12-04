import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AddressPeopleMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> { //LongWritable is file offset
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
	

