import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank3 {
	public static void main(String[] args) throws Exception {
		int i = 0;
		while (i<3){
		     Job job = new Job();
		     job.setJarByClass(PageRank3.class);
		     
		     job.setJobName("page rank 3");
		     
		     
			 FileInputFormat.addInputPath(job, new Path(args[i]));
			 FileOutputFormat.setOutputPath(job, new Path(args[i+1]));

		     job.setMapperClass(PageRankMapper.class);
		     job.setReducerClass(PageRankReducer.class);
		     job.setNumReduceTasks(1);
		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(Text.class);
		     
		     // Submit the job, then poll for progress until the job is complete
		     if(job.waitForCompletion(true)) i++;
		}
		System.exit(i==3?0:1);

	}
}
