/**
 * JoinReviewMapper is the 2nd mapper class for join mapper reduce
 * 
 * @author Tianhui Zhu, Hao Wu
 * */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinReviewMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  // A ReviewDataParser parser is created for future use of amazon review data
  private ReviewDataParser parser = new ReviewDataParser();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	//the parser parses value, which is a line of review data
  	parser.parse(value);
    //A TextPair object is created with String"1" as the second value
    //The alphabetical order of "1" makes this mapper arrive reducer first
     context.write(new TextPair(parser.getReviewAsin(), "1"), value);
  }
}