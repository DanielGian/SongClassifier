package Sorter;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] word = line.split("\\s");
    String pop = "";
    String name = "";
    
    if (word.length > 1){
    	pop = word[0];
    	for(int i = 1; i < word.length; i++){
    		name += word[i];
    	}
    	context.write(new FloatWritable(Float.parseFloat(pop)), new Text(name));
    }
  }
}