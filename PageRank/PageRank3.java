import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRank3 {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <input path> <output path>");
			System.exit(-1);
		}
		Path path1 = new Path (args[0]);
		Path path2 = new Path (args[1]);
		for(int i = 1; i<=3; i++){
			JobConf conf = new JobConf(PageRank3.class);
			conf.setJobName("Page Rank 3 ");

			FileInputFormat.addInputPath(conf, path1);
			FileOutputFormat.setOutputPath(conf, path2);
			conf.setMapperClass(PageRankMapper.class);
			conf.setReducerClass(PageRankReducer.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			JobClient.runJob(conf);
			path1 = path2;
			String temppath = path2.toString()+i;
			path2 = new Path(temppath);
		}
	}
}