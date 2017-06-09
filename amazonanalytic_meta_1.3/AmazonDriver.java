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
 * this is map-reduce class for Amazon metadata, in Map class, various methods are coded to extract
 * different fields from one metadata entry
 **/
public class AmazonDriver{
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	 /**
	  * @param:String line
	  * @return:String asin
	  *   this method extracts field "asin" from Amazon_metadata
	  */
    private String getAsin(String line){
      String asin ="";
      String delimiter_asin = "asin\': \'";
      String [] asin_split = line.split(delimiter_asin);
    //first split with "asin" remove anything before field "asin"
      if(asin_split.length<=1)
        return asin;//if this entry doesn't have field "asin", return ""
      String [] asin_split_after = asin_split[1].split("\'");
    //second split, remove anything after field "asin"
      asin = asin_split_after[0];
      return asin;
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "title" from Amazon_metadata
     */
    private String getTitle(String line){
      String title ="";                
      String delimiter_title = "title\': \'";
      String [] title_split = line.split(delimiter_title);
    //first split with "title" remove anything before field "title"
      if (title_split.length<=1)
        return title;//if this entry doesn't have field "title", return ""
      String [] title_split_after = title_split[1].split("\'");
    //second split, remove anything after field "title"
      title = title_split_after[0];
      return title;      
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "price" from Amazon_metadata
     */
    private String getPrice(String line){
      String price = "";
      String delimiter_price = "price\': ";
      String [] price_split = line.split(delimiter_price);
    //first split with "pirce" remove anything before field "price"
      if(price_split.length<=1)
        return price;//if this entry doesn't have field "price", return ""
      String [] price_split_after = price_split[1].split(",");
    //second split, remove anything after field "price"
      price = price_split_after[0];
      return price;
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "sales rank number" from Amazon_metadata
     */
    private String getSalesRankNum(String line){
      String salesranknum = "";
      String delimiter_salesrank = "salesRank\': ";
      String [] salesrank_split = line.split(delimiter_salesrank);
    //first split with "salesRank" remove anything before field "sales rank"
      if(salesrank_split.length <= 1){
        return salesranknum;//if this entry doesn't have field "sales rank", return ""
      }
      String [] salesrank_split_after = salesrank_split[1].split(":");
    //second split, remove anything before field "sales rank number"
      if (salesrank_split_after.length <= 1)
        return salesranknum;//if this entry doesn't have field "sales rank number ", return ""
      String [] temp = salesrank_split_after[1].split("}");
    //third split, remove anything after field "sales rank number"
      return salesranknum = temp[0];
    } 
    /**
     * @param: args input output
     * @param: Stirng line
     * @return: String title
     *   this method extract field "sales rank category" from Amazon_metadata
     */
    private String getSalesRankCategory(String line){
      String salerankcategory = "";
      String delimiter_salesrank = "salesRank\': ";
      String [] salesrank_split = line.split(delimiter_salesrank);
    //first split with "salesRank" remove anything before field "sales rank"
      if(salesrank_split.length <= 1){
        return salerankcategory;//if this entry doesn't have field "sales rank", return ""
      }
      String [] salesrank_split_after = salesrank_split[1].split("\'");
    //second split, remove anything before field "sales rank category"
      if(salesrank_split_after.length <= 1)
        return salerankcategory;//if this entry doesn't have field "sales rank category", return ""
      return salerankcategory = salesrank_split_after[1];
    //third split, remove anything after field "sales rank category"
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "brand" from Amazon_metadata
     */
    private String getBrand(String line){
      String brand = "";
      String delimiter_brand = "brand\': ";
      String [] brand_split = line.split(delimiter_brand);
    //first split with "brand" remove anything before field "brand"
      if (brand_split.length <= 1)
        return brand;//if this entry doesn't have field "brand", return ""
      String [] brand_split_after = brand_split[1].split("\'");
    //second split, remove anything after field "brand"
      return brand = brand_split_after[1];
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "category" from Amazon_metadata
     */
    private String getCategory(String line){
      String category = "";
      String delimiter_category = "categories\': ";
      String [] category_split = line.split(delimiter_category);
    //first split with "category" remove anything before field "category"
      if (category_split.length <= 1)
        return category;//if this entry doesn't have field "category", return ""
      String [] category_split_after = category_split[1].split("\'");
    //second split, remove anything after field "category"
      return category= category_split_after[1];
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "also bought" from Amazon_metadata
     */
    private String getAlsoBought(String line){
      String alsobought = "";
      String delimiter_alsobought = "also_bought\': ";
    //first split with "alsobought" remove anything before field "alsobought"
      String []alsobought_split = line.split(delimiter_alsobought);
      if (alsobought_split.length <=1)
        return alsobought;//if this entry doesn't have field "also bought", return ""
      String[] alsobought_split_after = alsobought_split[1].split("\']");
    //second split, remove anything after field "alsobought"
      String alsobought_list_one = alsobought_split_after[0];
      String temp = alsobought_list_one.substring(2, alsobought_list_one.length());//reomve " " and "["
      String []temp_list = temp.split("\', \'");
    //third split, if field "also bought" has more than one item 
      for (int i = 0; i < temp_list.length; i++){
        alsobought = alsobought+temp_list[i]+',';
      }//if field "also bought" has more than one item, loop through and concatenate all into one string, seperated with ","
      return alsobought;
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "also view" from Amazon_metadata
     */
    private String getAlsoView(String line){
      String alsoviewed = "";
      String delimiter_alsoviewed = "also_viewed\': ";
      String []delimiter_alsoviewed_split = line.split(delimiter_alsoviewed);
    //first split with "alsoview" remove anything before field "also view"
      if (delimiter_alsoviewed_split.length <=1)
        return alsoviewed;//if this entry doesn't have field "also view", return ""
      String[] delimiter_alsoviewed_split_after = delimiter_alsoviewed_split[1].split("\']");
    //second split, remove anything after field "also view"
      String alsoviewed_list_one = delimiter_alsoviewed_split_after[0];
      String temp = alsoviewed_list_one.substring(2, alsoviewed_list_one.length());//reomve " " and "["
      String []temp_list = temp.split("\', \'");
    //third split, if field "also view" has more than one item
      for (int i = 0; i < temp_list.length; i++){
        alsoviewed = alsoviewed+temp_list[i]+',';
      }//if field "also view" has more than one item, loop through and concatenate all into one string, seperated with ","
      return alsoviewed;
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "bought together" from Amazon_metadata
     */
    private String getBoughtTogether(String line){
      String boughttogether = "";
      String delimiter_boughttogether = "bought_together\': ";
      String []delimiter_boughttogether_split = line.split(delimiter_boughttogether);
    //first split with "bought_together" remove anything before field "also view"
      if (delimiter_boughttogether_split.length <=1)
        return boughttogether;//if this entry doesn't have field "bought together", return ""
      String[] delimiter_boughttogether_split_after = delimiter_boughttogether_split[1].split("\']");
    //second split, remove anything after field "bought together"
      String boughttogether_list_one = delimiter_boughttogether_split_after[0];
      String temp = boughttogether_list_one.substring(2, boughttogether_list_one.length());//reomve " " and "["
      String []temp_list = temp.split("\', \'");
    //third split, if field "bought together" has more than one item
      for (int i = 0; i < temp_list.length; i++){
        boughttogether = boughttogether+temp_list[i]+',';
      }//if field "bought together" has more than one item, loop through and concatenate all into one string, seperated with ","
      return boughttogether;
    }
    /**
     * @param: Stirng line
     * @return: String title
     *   this method extract field "bought after view" from Amazon_metadata
     */
    private String getBuyAfterView(String line){
      String buyafterview = "";
      String delimiter_buyafterview = "buy_after_viewing\': ";
      String []delimiter_buyafterview_split = line.split(delimiter_buyafterview);
    //first split with "buy_after_viewing" remove anything before field "buy after view"
      if (delimiter_buyafterview_split.length <=1)
        return buyafterview;//if this entry doesn't have field "buy after view", return ""
      String[] delimiter_buyafterview_split_after = delimiter_buyafterview_split[1].split("\']");
    //second split, remove anything after field "buy after view"
      String buyafterview_list_one = delimiter_buyafterview_split_after[0];
      String temp = buyafterview_list_one.substring(2, buyafterview_list_one.length());//reomve " " and "["
      String []temp_list = temp.split("\', \'");
    //third split, if field "buy after view" has more than one item
      for (int i = 0; i < temp_list.length; i++){
        buyafterview = buyafterview+temp_list[i]+',';
      }//if field "buy after view" has more than one item, loop through and concatenate all into one string, seperated with ","
      return buyafterview;
    }
    /**
     * @param: LongWritable key, Text value, Context context
     *  this mapper write key value pair to mapper output.
     */
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{      
        String line = value.toString();
        context.write (new Text(getAsin(line)), new Text (getPrice(line)));
        //by calling different method from Map class, we can extract different key pair combo
    }
}
  public  static class Reduce extends Reducer<Text, Text, Text, Text> {
	  /**
	   * @param: Text key, Iterable<Text> values, Context context
	   *   this is reducer, write key value pair to output
	   */
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