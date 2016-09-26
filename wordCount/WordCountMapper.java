import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		String line = value.toString();
		line = line.toLowerCase();

		String word1 = "chicago",word2 = "dec",word3 = "java",word4 = "hackathon";
	
		int count1 = 0,count2 = 0,count3 = 0,count4 = 0;

		if(line.contains(word1)) count1++; context.write(new Text(word1), new IntWritable(count1));
		if(line.contains(word2)) count2++; context.write(new Text(word2), new IntWritable(count2));
		if(line.contains(word3)) count3++; context.write(new Text(word3), new IntWritable(count3));
		if(line.contains(word4)) count4++; context.write(new Text(word4), new IntWritable(count4));
		
	}
}
	


