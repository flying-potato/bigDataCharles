import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AddressPeopleMapReduce {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: AddressPeople <input path> <output path>");
			System.exit(-1);
			}
		JobConf conf = new JobConf(AddressPeople.class);
		conf.setJobName("AddressPeople");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapperClass(AddressPeopleMapper.class);
		conf.setReducerClass(AddressPeopleReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
	}
}
