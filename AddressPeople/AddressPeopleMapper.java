import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AddressPeopleMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
		try{

			String line = value.toString();

			String[] sp = line.split(",");
			String timeadd =  sp[0]+","+sp[1];
			String peoplestr = sp[2];
			int people = Integer.parseInt(peoplestr);

			output.collect(new Text(timeadd), new IntWritable(people));
		}catch(Exception e){
			System.out.println(e);
		}
	}

}