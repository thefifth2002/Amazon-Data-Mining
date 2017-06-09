/**
 * ReviewDataParser is the parser of raw review data
 * all get methods return corresponding String in the meta data
 * 
 * @author Tianhui Zhu, Hao Wu
 * */

import org.apache.hadoop.io.Text;

public class ReviewDataParser { 
  public String line = "";
  
  /**
   * parse method parse a string line input
   * 
   *@param line: a String line of input data
   * */
  public void parse(String line){
    this.line = line;
  }
  
  /**
   * parse method parse a Text record input
   * 
   *@param record: a Text record
   * */
  public void parse(Text record){
    parse(record.toString());
  }
  
  /**
   * getReviewAsin method get the asin of the review
   * 
   *@return asin of the review
   * */
  public String getReviewAsin(){
    String asin = "";
    String delimiter_asin = "asin\": ";
    String [] asin_split = line.split(delimiter_asin);
    if (asin_split.length <= 1){
      return asin;
    }    	
    String [] asin_split_after = asin_split[1].split("\"");
    return asin = asin_split_after[1];
  }

  /**
   * getReiewID method get the reviewerID of the review
   * 
   *@return reviewerID of the review
   * */
  public String getReiewID(){
    String reviewerID = "";
    String dqcomma = "\", \"";
    String dqcolon = "\": \"";
    String [] comma_split = line.split(dqcomma, 2);
    if (comma_split.length <= 1){
      return reviewerID;
    }    	
    String [] reviewerID_split = comma_split[0].split(dqcolon);
    return reviewerID = reviewerID_split[1];    	
  }
  
  /**
   * getReviewerName method get the reviewerName of the review
   * 
   *@return reviewerName of the review
   * */ 
  public String getReviewerName(){
    String reviewerName = "";
    String delimiter_reviewerName = "reviewerName\": \"";
    String [] reviewerName_split = line.split(delimiter_reviewerName);
    if (reviewerName_split.length <= 1){
      return reviewerName;
    }    	
    String [] reviewerName_split_after = reviewerName_split[1].split("\", \"");
    return reviewerName = reviewerName_split_after[0];
  }

  /**
   * getOverall method get the overall rating of the review
   * 
   *@return overall rating of the review
   * */ 
  public String getOverall(){
    String overall = "";
    String delimiter_overall = "overall\": ";
    String [] overall_split = line.split(delimiter_overall);
    if (overall_split.length <= 1){
      return overall;
    }    	
    String [] overall_split_after = overall_split[1].split(",");
    return overall = overall_split_after[0];
 }

  /**
   * getHelpful method get the helpful rating of the review
   * 
   *@return helpful of the review
   * */ 
  public String getHelpful(){
    String helpful = "";
    String delimiter_helpful = "helpful\": ";
    String [] helpful_split = line.split(delimiter_helpful);
    if (helpful_split.length <= 1){
      return helpful;
    }    	
    String [] helpful_split_after = helpful_split[1].split(", \"");
    String helpful_temp = helpful_split_after[0];
    String temp = helpful_temp.substring(1, helpful_temp.length()-1);
    String [] temp_arr = temp.split(",");
    String first = temp_arr[0];
    String second = temp_arr[1];
    int firstint = Integer.parseInt(first.trim());
    int secondint = Integer.parseInt(second.trim());
    if (secondint == 0){
      helpful = "" + 1;
    }else{
      helpful = "" + firstint * 3;
    }    	
    return helpful;
  }
  
  /**
   * getReviewTime method get the year of the review
   * 
   *@return year of the review
   * */ 
  public String getReviewTime(){
    String reviewtime = "";
    String year = "";
    String delimiter_reviewtime = "\"reviewTime\": ";
    String [] reviewtime_split = line.split(delimiter_reviewtime);
    if (reviewtime_split.length <= 1){
      return reviewtime;
    }   	
    String [] reviewtime_split_after = reviewtime_split[1].split("\"");
    year = reviewtime_split_after[1].substring(6);
    return reviewtime_split_after[1] + year;
  }  
}