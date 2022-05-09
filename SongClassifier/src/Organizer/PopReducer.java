package Organizer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PopReducer extends Reducer<Text, FloatWritable, FloatWritable, Text> {

  @Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float wordCount = 0;

		for (FloatWritable value : values) {
		  
			wordCount += value.get();
		}
		
		context.write(new FloatWritable(wordCount), key);
	}
}