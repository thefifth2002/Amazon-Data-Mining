/**
 * JoinMetaMapper is the 1st mapper class for join mapper reduce
 * 
 * @author Tianhui Zhu, Hao Wu
 * */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMetaMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  // A MetaDataParser parser is created for future use of amazon meta data
  private MetaDataParser parser = new MetaDataParser();  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	//the parser parses value, which is a line a meta data
    parser.parse(value);
    //A TextPair object is created with String"0" as the second value
    //The alphabetical order of "0" makes this mapper arrive reducer first
    context.write(new TextPair(parser.getAsin(), "0"), value);
  }
}