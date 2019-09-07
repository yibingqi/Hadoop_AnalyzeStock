import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class AnalyzeStock {
	  public static class StockMapper
      extends Mapper<Object, Text, Text, Text>{
      private String name;
   @Override
   protected void setup(Context context)
           throws IOException, InterruptedException {
       Configuration conf = context.getConfiguration();
       name = conf.get("name");
   }
   public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
   	String line = value.toString();
   	String[] SingleStockData = line.split(",");
    String stock = null;
   	Text one = null;
   	String pricename=null;
   	if(name == "close"){pricename=SingleStockData[5];}
   	if(name == "low"){pricename=SingleStockData[4];}
   	if(name == "high"){pricename=SingleStockData[3];}

		  stock = SingleStockData[0];

		
		  if (pricename.trim().length()==0) {pricename = "0";}
		  one = new Text(SingleStockData[1] + "," + pricename);
	context.write(new Text(stock),one);
    }
 }
	public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
	 private Text result = new Text();

     public void reduce(Text key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {
         String startdate1=null;
         String enddate1=null;
         Configuration conf = context.getConfiguration();
         startdate1 = conf.get("startdate");
         enddate1 = conf.get("enddate");
 
         boolean flag=false; 
		 double maxPrice = Float.MIN_VALUE;
		 String aggregation=null;

		 double minPrice = Float.MAX_VALUE;

		 String finalresult = null;
         double sum = 0;
         double i=0;
         aggregation = conf.get("aggregation");
   
         if (aggregation=="avg"){
              for (Text val : values) {
      			String valueInString=val.toString();
    			String valueArray[]=valueInString.split(",");
    			String dateInString=valueArray[0];
    			double val1=Double.parseDouble(valueArray[1]);
    			SimpleDateFormat formatter=new SimpleDateFormat("MM/dd/yyyy");
    			SimpleDateFormat formatter2=new SimpleDateFormat("yyyy-MM-dd");
    			Date date=new Date();
    			Date startdate=new Date();
    			Date enddate=new Date();
    			try
    			{
    				date=formatter2.parse(dateInString);
    				startdate=formatter.parse(startdate1);
    				enddate=formatter.parse(enddate1);
    			}
    			catch(ParseException e)
    			{
    				e.printStackTrace();
    			}
    			flag=false;
    			if(date.after(startdate)||date.equals(startdate))
    			{
    				flag=true;
    			}
    			if(date.after(enddate))
    			{
    				flag=false;
    			}
    			if(flag){
                 sum += val1;
                 i=i+1;}
             }
             double avg = sum / i; 
        	 finalresult = Double.toString(avg);
         }
         if (aggregation=="max"){
             for (Text val : values) {
     		 String valueInString=val.toString();
   			 String valueArray[]=valueInString.split(",");
   			 String dateInString=valueArray[0];
   			 double val1=Double.parseDouble(valueArray[1]);
   			 SimpleDateFormat formatter=new SimpleDateFormat("MM/dd/yyyy");
   			 SimpleDateFormat formatter2=new SimpleDateFormat("yyyy-MM-dd");
   			 Date date=new Date();
   			 Date startdate=new Date();
   			 Date enddate=new Date();
   			 try
   			 {
   				date=formatter2.parse(dateInString);
   				startdate=formatter.parse(startdate1);
   				enddate=formatter.parse(enddate1);
   			 }
   			 catch(ParseException e)
   			 {
   				e.printStackTrace();
   			 }
   			 flag=false;
   			if(date.after(startdate)||date.equals(startdate))
   			 {
   				flag=true;
   			 }
   			 if(date.after(enddate))
   			 {
   				flag=false;
   			 }
   			if(flag){
   			 maxPrice = Math.max(maxPrice, val1);}
             }
             finalresult = Float.toString((float) maxPrice);
         }
         if (aggregation=="min"){
             for (Text val : values) {
     		 String valueInString=val.toString();
   			 String valueArray[]=valueInString.split(",");
   			 String dateInString=valueArray[0];
   			 double val1=Double.parseDouble(valueArray[1]);
   			 SimpleDateFormat formatter=new SimpleDateFormat("MM/dd/yyyy");
   			 SimpleDateFormat formatter2=new SimpleDateFormat("yyyy-MM-dd");
   			 Date date=new Date();
   			 Date startdate=new Date();
   			 Date enddate=new Date();
   			 try
   			 {
   				date=formatter2.parse(dateInString);
   				startdate=formatter.parse(startdate1);
   				enddate=formatter.parse(enddate1);
   			 }
   			 catch(ParseException e)
   			 {
   				e.printStackTrace();
   			 }
   			 flag=false;
   			if(date.after(startdate)||date.equals(startdate))
   			 {
   				flag=true;
   			 }
   			 if(date.after(enddate))
   			 {
   				flag=false;
   			 }
   			if(flag){
   			 minPrice = Math.min(minPrice, val1);}
             }
             finalresult = Float.toString((float) minPrice);
         }
     result.set(finalresult);
     context.write(key, new Text(result));
   }
 }


   public static void main(String[] args) throws Exception {
	/*String caseSensitive = args[0];*/
	  
      Configuration conf = new Configuration();
      conf.set("startdate",args[0]);
      conf.set("enddate",args[1]);
      conf.set("aggregation",args[2]);
      conf.set("name",args[3]);
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(AnalyzeStock.class);
    job.setMapperClass(StockMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPaths(job, new String(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

