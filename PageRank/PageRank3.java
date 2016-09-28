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
		int i=0;
		while( i<3 ){
			Job job = new Job();
			Configuration conf = job.getConfiguration();
			conf.set("mapreduce.output.textoutputformat.separator","");
			job.setJarByClass(PageRank3.class);
			job.setJobName("Page Rank 3 times");

			// JobConf conf = new JobConf(PageRank3.class);
			// conf.setJobName("Page Rank 3 ");
			Path path1 = new Path (args[i+0]);
			Path path2 = new Path (args[i+1]);
			FileInputFormat.addInputPath(conf, path1);
			FileOutputFormat.setOutputPath(conf, path2);
			job.setNumReduceTasks(1);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			if(job.waitForCompletion(true)){
				i++;
			}
		}
	}
}