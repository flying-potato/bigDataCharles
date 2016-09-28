import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class PageRank3 {
	public static void main(String[] args) throws IOException {
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
		     Job job = Job.getInstance();
		     job.setJarByClass(PageRank3.class);
		     
		     // Specify various job-specific parameters     
		     job.setJobName("myjob");
		     
		     job.setInputPath(new Path(args[i]));
		     job.setOutputPath(new new Path(args[i+1]));
		     
		     job.setMapperClass(MyJob.MyMapper.class);
		     job.setReducerClass(MyJob.MyReducer.class);

		     job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);
			 job.setNumReduceTasks(1);
		     // Submit the job, then poll for progress until the job is complete
		     if(job.waitForCompletion(true)) i++;
		}
		System.exit(i==3?0:1);

	}
}