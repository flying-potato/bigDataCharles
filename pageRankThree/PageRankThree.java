import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankThree {
	public static void main(String[] args) throws IOException {
	  int i = 0;
	  while (i<3){
		JobConf conf = new JobConf(PageRankThree.class);
		conf.setJobName("Page Rank Three");
		FileInputFormat.addInputPath(conf, new Path(args[i]));
		FileOutputFormat.setOutputPath(conf, new Path(args[i+1]));
		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setNumReduceTasks(1);

		JobClient.runJob(conf);
		i++;
	  }	
	}
}
