import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
/*
*@author: Hao Wu, Tianhui Zhu
* this is map-reduce word count for processed Amazon meta and review key value pair
* every line of input contains one key and 0 or more values, values are separated by \t or " "
**/
public class WordCount{
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	  /**
	   * @param: Text key, Iterable<Text> values, Context context
	   *   this is mapper, write key value pair to output
	   */
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{      
        String line = value.toString();
        line = line.replaceAll("\t", ",");
        line = line.replaceAll(" ", ",");
        String[] clean_arr = line.split(",");
        String asin_word = "";
        for (int i = 0; i < clean_arr.length; i++){
          asin_word = clean_arr[i];
          context.write (new Text(asin_word ), new IntWritable (1));
        }
    }
}
  public  static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	  /**
	   * @param: Text key, Iterable<Text> values, Context context
	   *   this is reducer, write key value pair to output
	   */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {          
      int counter = 0;
      for (IntWritable value: values){
        counter+=value.get();
      }
     context.write(key, new IntWritable(counter));
    }
  }
  /**
   * @param: args input output
   * */
  public static void main(String[] args) throws Exception {   
    if (args.length != 2) {
      System.err.println("Usage: hadoop PageRankDriver <input_path> <output_path>");
      System.exit(-1);
    }
    Job job = new Job();
    job.setJobName("word count");
    job.setJarByClass(WordCount.class);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}