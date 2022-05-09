package Organizer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PopMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] song = line.split(",");
    if(song.length == 22){
    	String all = song[1] + "||||" + song[9] + "||||" + song[10] + "||||" + song[12] + "||||"
        + song[14] + "||||" + song[15] + "||||" + song[17] + "||||" + song[18] + "||||" + song[19];
    	context.write(new Text(all), new FloatWritable((float) 100 / (float) Integer.parseInt(song[0])));  
    }
    
  }
}