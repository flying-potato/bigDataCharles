import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
		String line = value.toString();

		String[] sp = line.split(" ");
		String source = sp[0];
		String[] despage = Arrays.copyOfRange(sp, 1, sp.length-1);
		double PRval = sp[sp.length - 1];

		String descomb = despage[0];
		for(int i=1; i<despage.length; i++){
			descomb = descomb +" "+despage[i];
		}
		output.collect(new Text(source), new Text(descomb));
		
		int dessize = despage.length;
		for (int i=0; i<despage.length; i++){
			String valuefordes = source +" "+ Double.toString(PRval/dessize);
			output.collect(new Text(despage[i]), new Text(valuefordes));
		}
	}

}