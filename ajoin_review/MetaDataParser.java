/**
 * MetaDataParser is the parser of raw metadata
 * all get methods return corresponding String in the metadata
 * 
 * @author Tianhui Zhu, Hao Wu
 * */

import org.apache.hadoop.io.Text;

public class MetaDataParser {  
  public String line = "";
  
  /**
   * parse method parse a string line input
   * 
   *@param line: a String line of input data
   * */
  public void parse(String line) {
  	this.line = line;
  }
  
  /**
   * parse method parse a Text record input
   * 
   *@param record: a Text record
   * */
  public void parse(Text record) {
    parse(record.toString());
  }
  
  /**
   * getAsin returns the asin of this parser
   * 
   *@return asin
   * */
  public String getAsin(){
    String asin ="";	
    String delimiter_asin = "asin\': \'";
    String [] asin_split = line.split(delimiter_asin);
    if(asin_split.length <= 1){
      return asin;
    }    	
    String [] asin_split_after = asin_split[1].split("\'");
    asin = asin_split_after[0];
    return asin;                        	
  }
  
  /**
   * getPrice returns the price
   * 
   * @return price
   * */
  public String getPrice(){	
    String price = "";
    String delimiter_price = "price\': ";
    String [] price_split = line.split(delimiter_price);
    if(price_split.length <= 1){
      return price;
    }
    String [] price_split_after = price_split[1].split(",");
    price = price_split_after[0];
    return price;	
  }
  
  /**
   * getTitle returns the title
   * 
   * @return title
   * */
  public String getTitle(){
    String title ="";                
    String delimiter_title = "title\': \'";
    String [] title_split = line.split(delimiter_title);
    if (title_split.length<=1){
      return title;
    }    	
    String [] title_split_after = title_split[1].split("\'");
    title = title_split_after[0];    
    return title;    
  }
  
  /**
   * getSalesRankNum returns the salesranknum
   * 
   * @return salesranknum
   * */
  public String getSalesRankNum(){
    String salesranknum = "";
    String delimiter_salesrank = "salesRank\': ";
    String [] salesrank_split = line.split(delimiter_salesrank);
    if(salesrank_split.length <= 1){
      return salesranknum;
    }
    String [] salesrank_split_after = salesrank_split[1].split(":");
    if (salesrank_split_after.length <= 1){
      return salesranknum;
    }    
    String [] temp = salesrank_split_after[1].split("}");
    return salesranknum = temp[0];
  }
  
  /**
   * getSalesRankCategory returns the salerankcategory
   * 
   * @return salerankcategory
   * */
  public String getSalesRankCategory(){
    String salerankcategory = "";
    String delimiter_salesrank = "salesRank\': ";    
    String [] salesrank_split = line.split(delimiter_salesrank);
    if(salesrank_split.length <= 1){
      return salerankcategory;
    }
    String [] salesrank_split_after = salesrank_split[1].split("\'");
    if(salesrank_split_after.length <= 1){
      return salerankcategory;
    }    	
    return salerankcategory = salesrank_split_after[1];
  }

  /**
   * getBrand returns the brand
   * 
   * @return brand
   * */
  public String getBrand(){
    String brand = "";
    String delimiter_brand = "brand\': ";
    String [] brand_split = line.split(delimiter_brand);
    if (brand_split.length <= 1){
      return brand;
    }    	
    String [] brand_split_after = brand_split[1].split("\'");
    return brand = brand_split_after[1];
  }

  /**
   * getCategory returns the category
   * 
   * @return category
   * */
  public String getCategory(){
    String category = "";
    String delimiter_category = "categories\': ";
    String [] category_split = line.split(delimiter_category);
    if (category_split.length <= 1){
      return category;
    }    	
    String [] category_split_after = category_split[1].split("\'");
    return category= category_split_after[1];
  }

  /**
   * getAlsoBought returns the alsobought
   * 
   * @return alsobought
   * */
  public String getAlsoBought(){
    String alsobought = "";
    String delimiter_alsobought = "also_bought\': ";
    String []alsobought_split = line.split(delimiter_alsobought);
    if (alsobought_split.length <=1){
      return alsobought;
    }    	
    String[] alsobought_split_after = alsobought_split[1].split("\']");
    String alsobought_list_one = alsobought_split_after[0];
    String temp = alsobought_list_one.substring(2, alsobought_list_one.length());
    String []temp_list = temp.split("\', \'");
    for (int i = 0; i < temp_list.length; i++){
      alsobought = alsobought+temp_list[i]+',';
    }
    return alsobought;    	
  }

  /**
   * getAlsoView returns the alsoviewed
   * 
   * @return alsoviewed
   * */
  public String getAlsoView(){
    String alsoviewed = "";
    String delimiter_alsoviewed = "also_viewed\': ";
    String []delimiter_alsoviewed_split = line.split(delimiter_alsoviewed);
    if (delimiter_alsoviewed_split.length <=1){
      return alsoviewed;
    }    	
    String[] delimiter_alsoviewed_split_after = delimiter_alsoviewed_split[1].split("\']");
    String alsoviewed_list_one = delimiter_alsoviewed_split_after[0];
    String temp = alsoviewed_list_one.substring(2, alsoviewed_list_one.length());
    String []temp_list = temp.split("\', \'");
    for (int i = 0; i < temp_list.length; i++){
      alsoviewed = alsoviewed+temp_list[i]+',';
    }
    return alsoviewed;    	
	}

  /**
   * getBoughtTogether returns the boughttogether
   * 
   * @return boughttogether
   * */
  public String getBoughtTogether(){
    String boughttogether = "";
    String delimiter_boughttogether = "bought_together\': ";
    String []delimiter_boughttogether_split = line.split(delimiter_boughttogether);
    if (delimiter_boughttogether_split.length <=1){
      return boughttogether;
    }      
    String[] delimiter_boughttogether_split_after = delimiter_boughttogether_split[1].split("\']");
    String boughttogether_list_one = delimiter_boughttogether_split_after[0];
    String temp = boughttogether_list_one.substring(2, boughttogether_list_one.length());
    String []temp_list = temp.split("\', \'");
    for (int i = 0; i < temp_list.length; i++){
      boughttogether = boughttogether+temp_list[i]+',';
    }
    return boughttogether;
  }

  /**
   * getBuyAfterView returns the buyafterview
   * 
   * @return buyafterview
   * */
  public String getBuyAfterView(){
    String buyafterview = "";
    String delimiter_buyafterview = "buy_after_viewing\': ";
    String []delimiter_buyafterview_split = line.split(delimiter_buyafterview);
    if (delimiter_buyafterview_split.length <= 1){
      return buyafterview;
    }    	
    String[] delimiter_buyafterview_split_after = delimiter_buyafterview_split[1].split("\']");
    String buyafterview_list_one = delimiter_buyafterview_split_after[0];
    String temp = buyafterview_list_one.substring(2, buyafterview_list_one.length());
    String []temp_list = temp.split("\', \'");
    for (int i = 0; i < temp_list.length; i++){
      buyafterview = buyafterview+temp_list[i]+',';
    }
    return buyafterview;
  }
}
