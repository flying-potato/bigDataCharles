import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();

		String[] sp = line.split("\b|\t");
		String source = sp[0];
		String[] despage = Arrays.copyOfRange(sp, 1, sp.length-1);
		double PRval = Double.parseDouble(sp[sp.length - 1]);

		String descomb = despage[0];
		for(int i=1; i<despage.length; i++){
			descomb = descomb +" "+despage[i];
		}
		context.write(new Text(source), new Text(descomb));
		
		int dessize = despage.length;
		for (int i=0; i<despage.length; i++){
			String valuefordes = source +" "+ Double.toString(PRval/dessize);
			context.write(new Text(despage[i]), new Text(valuefordes));
		}
	}

}
