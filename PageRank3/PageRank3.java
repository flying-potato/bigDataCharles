import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank3 {
	public static void main(String[] args) throws Exception {
		int i = 0;
		// while (i<3){
		// 	JobConf conf = new JobConf(PageRank3.class);
		// 	conf.setJobName("Page Rank3");
		// 	FileInputFormat.addInputPath(conf, new Path(args[i+0]));
		// 	FileOutputFormat.setOutputPath(conf, new Path(args[i+1]));
		// 	conf.setMapperClass(PageRankMapper.class);
		// 	conf.setReducerClass(PageRankReducer.class);
		// 	conf.setOutputKeyClass(Text.class);
		// 	conf.setOutputValueClass(Text.class);
		// 	conf.setNumReduceTasks(1);
		// 	JobClient.runJob(conf);
		// 	i++;
		// }

		while (i<3){
		     Job job = new Job();
		     job.setJarByClass(PageRank3.class);
		     
		     job.setJobName("page rank 3");
		     
		     
	FileInputFormat.addInputPath(job, new Path(args[i+0]));
	FileOutputFormat.setOutputPath(job, new Path(args[i+1]));

		     job.setMapperClass(PageRankMapper.class);
		     job.setReducerClass(PageRankReducer.class);

		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(Text.class);
		     job.setNumReduceTasks(1);
		     // Submit the job, then poll for progress until the job is complete
		     if(job.waitForCompletion(true)) i++;
		}
		System.exit(i==3?0:1);

	}
}
