import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
/**
*@author: Hao Wu, Tianhui Zhu
* this is map-reduce class for Amazon review data, in Map class, various methods are coded to extract
* different fields from one review data entry
**/
public class AmazonDriver{
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	  /**
	   * @param:String line
	   * @return:String reviewasin
	   *   this method extracts field "review asin" from Amazon review data
	   */
    private String getReviewAsin(String line){
      String asin = "";
      String delimiter_asin = "asin\": ";
      String [] asin_split = line.split(delimiter_asin);
    //first split with "asin" remove anything before field "asin"
      if (asin_split.length <= 1)
        return asin;//if this entry doesn't have field "asin", return ""
      String [] asin_split_after = asin_split[1].split("\"");
    //second split, remove anything after field "asin"
      return asin= asin_split_after[1];
    }
    /**
	   * @param:String line
	   * @return:String reviewID
	   *   this method extracts field "review ID" from Amazon review data
	   */
    private String getReiewID(String line){
      String reviewerID = "";
      String dqcomma = "\", \"";
      String dqcolon = "\": \"";
      String [] comma_split = line.split(dqcomma, 2);
    //first split, remove anything before field "review ID"
      if (comma_split.length <= 1)
        return reviewerID;//if this entry doesn't have field "reivew ID", return ""
      String [] reviewerID_split = comma_split[0].split(dqcolon);
    //second split, remove anything after field "reivew ID"
      return reviewerID = reviewerID_split[1];      
    }
    /**
	   * @param:String line
	   * @return:String reviewername
	   *   this method extracts field "reviewer name" from Amazon review data
	   */
    private String getReviewerName(String line){
      String reviewerName = "";
      String delimiter_reviewerName = "reviewerName\": \"";
      String [] reviewerName_split = line.split(delimiter_reviewerName);
    //first split, remove anything before field "reviewer name"
      if (reviewerName_split.length <= 1)
        return reviewerName;//if this entry doesn't have field "reivew name", return ""
      String [] reviewerName_split_after = reviewerName_split[1].split("\", \"");
    //second split, remove anything after field "reivewer name"
      return reviewerName= reviewerName_split_after[0];
    }
    /**
	   * @param:String line
	   * @return:String overall
	   *   this method extracts field "overall rating" from Amazon review data
	   */
    private String getOverall(String line){
      String overall = "";
      String delimiter_overall = "overall\": ";
    //first split, remove anything before field "overall"
      String [] overall_split = line.split(delimiter_overall);
      if (overall_split.length <= 1)
        return overall;//if this entry doesn't have field "overall", return ""
      String [] overall_split_after = overall_split[1].split(",");
    //second split, remove anything after field "overall"
      return overall= overall_split_after[0];
    }
    /**
	   * @param:String line
	   * @return:String helpful
	   *   this method extracts field "helpful" from Amazon review data
	   */
    private String getHelpful(String line){
      String helpful = "";
      String delimiter_helpful = "helpful\": ";
      String [] helpful_split = line.split(delimiter_helpful);
    //first split, remove anything before field "helpful"
      if (helpful_split.length <= 1)
        return helpful;//if this entry doesn't have field "helpful", return ""
      String [] helpful_split_after = helpful_split[1].split(", \"");
    //second split, remove anything after field "helpful"
      String helpful_temp = helpful_split_after[0];
      String temp = helpful_temp.substring(1, helpful_temp.length()-1);//remove "]"
      String [] temp_arr = temp.split(",");
    //third split, get first number and second number
      String first = temp_arr[0];
      String second = temp_arr[1];
      int firstint = Integer.parseInt(first.trim());
      int secondint = Integer.parseInt(second.trim());//string to int
      if (secondint == 0)
        helpful = "" + 1;
      else 
        helpful = "" + firstint * 3;//calculate helpful
      return helpful;
    }
    /**
	   * @param:String line
	   * @return:String reviewtime
	   *   this method extracts field "reviewer time" from Amazon review data
	   */
    private String getReviewTime(String line){
      String reviewtime = "";
      String year = "";
      String delimiter_reviewtime = "\"reviewTime\": ";
      String [] reviewtime_split = line.split(delimiter_reviewtime);
    //first split, remove anything before field "review time"
      if (reviewtime_split.length <= 1)
        return reviewtime;//if this entry doesn't have field "review time", return ""
      String [] reviewtime_split_after = reviewtime_split[1].split("\"");
    //second split, remove anything after field "review time"
      year = reviewtime_split_after[1].substring(6); //get review time:year
      return reviewtime_split_after[1] + year; //return full review time + year, for future use
    }
    /**
     * @param: LongWritable key, Text value, Context context
     *  this mapper write key value pair to mapper output.
     */
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{      
        String line = value.toString();
        context.write (new Text(getReviewAsin(line)), new Text (getReviewTime(line)));
      //by calling different method from Map class, we can extract different key pair combo
    }
}
  /**
   * @param: Text key, Iterable<Text> values, Context context
   *   this is reducer, write key value pair to output
   */
  public  static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {     
     String result = "";
     String temp = "";
     for (Text value : values){
       temp = value.toString();
       result += temp + ", ";
    // values with the same key are concatenated into one string, seperated with ","
     }
     context.write(key, new Text(result));    
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
    job.setJobName("Amazon Analytics");
    job.setJarByClass(AmazonDriver.class);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}