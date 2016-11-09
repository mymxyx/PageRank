


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

  public static class InitMapper
       extends Mapper<Object, Text, Text, Text>{

	  public static enum Counter {
				NODE_NUM, MAX_OUTLINK, MIN_OUTLINK, ONEWAY_LINK_NUM
			}
	  //assume that rank for each page is 1 at the very beginning
	  protected void setup(Context context) throws IOException, InterruptedException {
			context.getCounter(Counter.MIN_OUTLINK).setValue(Long.MAX_VALUE);
		}
	  private static float startrank = 1.0f;
	  
	  
	  public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
		  
		  String  url = "";                          //__url == home page
		  float urlrank = startrank;                 
		  String outlink_list ="";                   //new String to store all the out links
		  int count_link = 0;                             //number of out-links
		  
		  StringTokenizer token = new StringTokenizer(value.toString()); 
		  if(token.hasMoreTokens()){
			  url= token.nextToken();		             // identify the home page
			  url+=" "+startrank;
			  context.getCounter(Counter.NODE_NUM).increment(1);
			  
			  
/*			  System.out.println("------------------------------------------------------------");
			  System.out.println("");
			  System.out.println("the input url is: "+ url);
			  System.out.println("");
			  System.out.println("------------------------------------------------------------");
*/			  
			  
			  while(token.hasMoreTokens()){
				  outlink_list += token.nextToken()+" ";
				  context.getCounter(Counter.ONEWAY_LINK_NUM).increment(1);
				  count_link++;
			  }
			  if(count_link > context.getCounter(Counter.MAX_OUTLINK).getValue()) {
					context.getCounter(Counter.MAX_OUTLINK).setValue(count_link);
				}
			  if(count_link < context.getCounter(Counter.MIN_OUTLINK).getValue()) {
					context.getCounter(Counter.MIN_OUTLINK).setValue(count_link);
				}
			  context.write(new Text(url), new Text(outlink_list));                //output format: <url page_rank> <page1 page2 page3...>
		  }
	  }
	  
	  protected void cleanup(Context context)throws IOException, InterruptedException
	    {	  
			double avg_outlink_num = (double)context.getCounter(Counter.ONEWAY_LINK_NUM).getValue() 
					/ (double)context.getCounter(Counter.NODE_NUM).getValue();
	        System.out.println("============================================");
	        System.out.println("Total node num:" + context.getCounter(Counter.NODE_NUM).getValue());
	        System.out.println("Average outlink num:" + avg_outlink_num);
	        System.out.println("max outlink num:" + context.getCounter(Counter.MAX_OUTLINK).getValue());
	        System.out.println("min outlink num:" + context.getCounter(Counter.MIN_OUTLINK).getValue());
	        System.out.println("Total one way link num：" + context.getCounter(Counter.ONEWAY_LINK_NUM).getValue());
	        System.out.println("============================================");
	          
	    }	
	  
	  
  }
  
  public static class PageRankMapper
  extends Mapper<Object, Text, Text, Text>{
 
	  public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
	  
	  String  url = "";                          //__url == home page
	  float prank = 0f;
	  String outlink_list =":";
	  float pr = 0f;
	  StringTokenizer token = new StringTokenizer(value.toString());  
	  if(token.hasMoreTokens()){
		  url= token.nextToken();                   // identify the home page
		  prank = new Float(token.nextToken());
		  context.write(new Text(url),new Text("#"+Float.toString(prank)));
		  
	      int outlink_count = token.countTokens();            //number of out links
		  if(outlink_count>0){
			  pr = prank/outlink_count;
		  }
		  while(token.hasMoreTokens()){
			  String outlink = token.nextToken();
			  outlink_list +=outlink+" ";
			  context.write(new Text(outlink), new Text("!"+Float.toString(pr)));
		  }
		  context.write(new Text(url), new Text(outlink_list));
/*		  
		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");
		  System.out.println("url= "+url);
		  System.out.println("prank= "+"#"+Float.toString(prank));
		  System.out.println("Float.toString(pr)= "+"!"+Float.toString(pr));
		  System.out.println("outlink_list= "+outlink_list);
		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");
		  */
	  }
	  }
  }
  
  
  public static class InitReducer
  extends Reducer<Text,Text,Text,Text> {

  public void reduce(Text key,Text values, Context context) throws IOException,InterruptedException{
	  
	  context.write(key,values);    //output key = <url new_rank>      output value= <page1 page2 page3 page4 ...> 
 }
}
  
  public static class PageRankReducer
       extends Reducer<Text,Text,Text,Text> {          //value  begin with ":" = outlink_list; begin with "#" = prank; begin with "!" = prank/outlink_count

	  public static enum Counter {
			CONV_CAL;
		}
	  public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		  float new_rank = 0f;
		  float pre_rank = 0f;
		  String newkey = "";
		  String outlink_list="";
//		  System.out.println("key= "+key);
		 
		  for(Text value: values){
			  String line = value.toString();
			  
			  if(line.contains(":")){                                //store outlilink_list
				  outlink_list= line.substring(1);
			  }
			  else if(line.contains("!")){                                                  //add up PageRank/numOfOutLink in outlink_list
				  new_rank += new Float(line.substring(1));
			  }
			  else if(line.contains("#")){
				  pre_rank = new Float(line.substring(1));
			  }
		  }
		  new_rank = (1-0.85f) + 0.85f*new_rank;                       //calculate the new rank
		  newkey = key.toString()+" "+ new_rank;
		  context.write(new Text(newkey),new Text(outlink_list));    //output key = <url new_rank>      output value= <page1 page2 page3 page4 ...> 
		  long scaledDelta = Math.abs((long) ((pre_rank - new_rank) * 1000.0));
		  
		  context.getCounter(Counter.CONV_CAL).increment(scaledDelta);
/*		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");

		  System.out.println("newkey= "+newkey);
		  System.out.println("outlink= "+outlink_list);
		  System.out.println("pre_rank= "+pre_rank);
		  System.out.println("new_rank= "+new_rank);
		  System.out.println("scaledDelta= "+scaledDelta);
		  System.out.println("CONV_CAL= "+context.getCounter(Counter.CONV_CAL).getValue());
		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");
*/		  
		  
	  }
  }

  public static class RankMapper
  extends Mapper<Object, Text, Text, NullWritable>{
 
	  public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
	  
	  String  url = "";                          //__url == home page
	  float pagerank = 0f;
	  StringTokenizer token = new StringTokenizer(value.toString());  
	  if(token.hasMoreTokens()){
		  url= token.nextToken();                   // identify the home page
		  pagerank = new Float(token.nextToken());
		  context.write(new Text(url+" "+Float.toString(pagerank)),NullWritable.get());
		  }
/*		  
		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");
		  System.out.println("url= "+url);
		  System.out.println("pagerank= "+"#"+Float.toString(pagerank));
		  System.out.println("");
		  System.out.println("-------------------------------------------------");
		  System.out.println("");
*/		  
	  }
  }
  
  public static class DescendingComp extends WritableComparator {
	    protected DescendingComp() {
	        super(Text.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        Text key1 = (Text) w1;
	        Text key2 = (Text) w2;      
	        FloatWritable n1 = new FloatWritable(Float.valueOf(key1.toString().split("\\s+")[1]));
	        FloatWritable n2 = new FloatWritable(Float.valueOf(key2.toString().split("\\s+")[1]));
	        
	        return -1 * n1.compareTo(n2);
	    }
	}
  
  public static void main(String[] args) throws Exception {
	  

    Configuration conf = new Configuration();
    String[] IOArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = Job.getInstance(conf, "Initialaze PageRank by Kayla");
    job.setJarByClass(PageRank.class);
    job.setMapperClass(InitMapper.class);
    job.setCombinerClass(InitReducer.class);
    job.setReducerClass(InitReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(IOArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(IOArgs[0] + "/../Ite_0"));
    job.waitForCompletion(true);
    
    Integer node_num = (int)job.getCounters().findCounter(InitMapper.Counter.NODE_NUM).getValue();
    Double avgoutlink = (double)job.getCounters().findCounter(InitMapper.Counter.ONEWAY_LINK_NUM).getValue()
    		/ (double)job.getCounters().findCounter(InitMapper.Counter.NODE_NUM).getValue();
    Integer maxoutlink = (int) job.getCounters().findCounter(InitMapper.Counter.MAX_OUTLINK).getValue();
    Integer minoutlink = (int) job.getCounters().findCounter(InitMapper.Counter.MIN_OUTLINK).getValue();
    Integer oneway_link_num = (int)job.getCounters().findCounter(InitMapper.Counter.ONEWAY_LINK_NUM).getValue();
    
    long startTime = System.currentTimeMillis();
    Integer iter_num=0;
    int count = 0;
    
    
    
    System.out.println("start iteration");
    //start iteration
    
    ArrayList<String> time = new ArrayList<>();
    
    
    for(count=0;;){
    	Configuration iteconf = new Configuration();
    	

        Job ite = Job.getInstance(conf, "Initialaze PageRank by Kayla");
        ite.setJarByClass(PageRank.class);
        ite.setMapperClass(PageRankMapper.class);
        ite.setReducerClass(PageRankReducer.class);
        
        ite.setOutputKeyClass(Text.class);
        ite.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(ite, new Path(IOArgs[0]+ "/../Ite_"+ count));
        FileOutputFormat.setOutputPath(ite, new Path(IOArgs[0] + "/../Ite_"+(++count)));
        ite.waitForCompletion(true);
        
        
        long Converge = ite.getCounters().findCounter(PageRankReducer.Counter.CONV_CAL).getValue();
//        System.out.println(Converge);
        if(Converge < 1) {
        	System.out.println("");
        	System.out.println("------------------------------------------------------------");
        	System.out.println("");
        	System.out.println("This is the "+count+ " iteration time");
        	System.out.println("PageRank has converged!");
        	System.out.println("");
        	System.out.println("------------------------------------------------------------");
        	System.out.println("");
        	break;
        }
        if(count > 50) {
        	System.err.println("failed to converge");
	        System.exit(2);
        }
        System.out.println("interation times:" + count);
        System.out.println("current scaled delta:" + Converge);
        Long timespent = new Long(System.currentTimeMillis()-startTime);
        time.add(Long.toString(timespent));
        System.out.println("Total time consumes:");
        System.out.println(Long.toString(timespent) + "ms");
    }
    
    
    System.out.println("");
    System.out.println("------------------------------------------------------------");
    System.out.println("");
    System.out.println("node_Num: "+node_num);
    System.out.println("avgoutlint_Num: "+avgoutlink);
    System.out.println("maxoutlink: "+maxoutlink);
    System.out.println("minoutlink: "+minoutlink);
    System.out.println("oneway_link_num: "+oneway_link_num);
    System.out.println("startTime: "+startTime);
    System.out.println("");
    System.out.println("------------------------------------------------------------");
    System.out.println("");
    System.out.println("==========================================");
    System.out.println("cost time: "+time);
    System.out.println("==========================================");
    
    //output result
    Configuration rankingConf = new Configuration();
	Job rankingJob = new Job(rankingConf, "ranking job hadoop-0.20");
	rankingJob.setJarByClass(PageRank.class);
	rankingJob.setJarByClass(PageRank.class);
	rankingJob.setMapperClass(RankMapper.class);
	rankingJob.setNumReduceTasks(1);
	rankingJob.setOutputKeyClass(Text.class);
	rankingJob.setOutputValueClass(NullWritable.class);
	rankingJob.setSortComparatorClass(DescendingComp.class);
	FileInputFormat.addInputPath(rankingJob, new Path(IOArgs[0]+ "/../Ite_"+ count));
	FileOutputFormat.setOutputPath(rankingJob, new Path(IOArgs[1]));
	rankingJob.waitForCompletion(true);
    System.out.println("Completed");
    
    
    
  }
}

