/**
 * JoinReducer is the reducer of this mapreduce program
 * 
 * 
 * @author Tianhui Zhu, Hao Wu
 * */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator<Text> iter = values.iterator();
    //give output only when iter hasNext
    //This only include metadata and review data both have the same asin
    while(iter.hasNext()){
      Text reviewLine = iter.next();
      ReviewDataParser reviewParser = new ReviewDataParser();
      reviewParser.parse(reviewLine);
      //String asin = reviewParser.getReviewAsin();
      String reviewID = reviewParser.getReiewID();
      String reviewerName = reviewParser.getReviewerName();
      String overAll = reviewParser.getOverall();
      String helpful = reviewParser.getHelpful();
      String reveiwTime = reviewParser.getReviewTime();
	  
      Text outValue = new Text(
	    //asin
	    //+ "\t" +
	    reviewID
	    + "\t" + reviewerName
	    + "\t" + overAll
	    + "\t" + helpful
	    + "\t" + reveiwTime	    		
	  );    	
      context.write(key.getFirst(), outValue);
    }   
  }
}