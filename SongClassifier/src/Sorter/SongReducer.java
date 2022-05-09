package Sorter;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {

  @Override
	public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  
		String song = "";

		for (Text value : values) {
		  
			song = value.toString();
			context.write(key, new Text(song));
		}
		
		
	}
}