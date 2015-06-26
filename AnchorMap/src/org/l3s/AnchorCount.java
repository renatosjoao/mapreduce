package org.l3s;

/***
 * 
 * @author  Renato Stoffalette Joao
 * @version 1.0
 * @since   2015-06 
 *
 ***/
 
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class AnchorCount extends Configured implements Tool {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    //private static final Logger logger = Logger.getLogger(AnchorCount.TokenizerMapper.class);

    private Text link = new Text();
    private Text anchor = new Text();
    private Map<String, String> anchor_srcURL = new HashMap<String,String>();
    
    @Override
    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    //logger.info("Running mapper");
    String[] line = value.toString().split("\t");	
    if(line.length > 4){
    	link.set(line[2]);
    	anchor.set("anchor:"+line[4]+",,, src_URL:"+line[0]+",,, dest_URL:"+line[2]);
    }else{
    	link.set(line[2]);
    	anchor.set("anchor:\t ,,, src_URL:"+line[0]+",,, dest_URL:"+line[2]);
    }
	context.write(link, anchor);

      }
    
    /** Read configuration (called once per mapper). */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
   }

    }
  

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
	private static final Logger logger = Logger.getLogger(AnchorCount.IntSumReducer.class);
	private Text result = new Text();
    Text anchor = new Text();
    Text t;
    int count = 0;
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
    //logger.info("Running Reducer");
    Text keyC = new Text();
    int count = 0;
    String translations = "";
    String[] splitable = null;
  
    for (Text val : values){
    	splitable = val.toString().split(",,,");
    	//should i consider empty anchors ???
    	//if(splitable[0].trim().equalsIgnoreCase("anchor:")){
    	//	continue;
    	//}
    	//else{
    	//translations += val.toString() +" ,";
    	translations += "\t"+splitable[0]+" "+splitable[1];
    	count++;
    	//}
    }

    translations +=  "\t f:" + Integer.toString(count);
    result.set(translations);
    keyC.set("dest_URL:"+key.toString());
    context.write(keyC, result);

    }
  }
  

  
  public static void main(String[] args) throws Exception {
      // use Hadoop runner to process default arguments
      int res = ToolRunner.run(new AnchorCount(), args);
      System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
// create Hadoop configuration
    Configuration conf = getConf();
	FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(args[1]), true);
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 10_000);
//Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Anchor Count");
//FileSystem.getLocal(conf).delete(new Path(args[1]), true);
    job.setJarByClass(AnchorCount.class);
    job.setMapperClass(TokenizerMapper.class);    
//job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(5);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
//job.setInputFormatClass(KeyValueTextInputFormat.class);   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
 // set up compression of output files
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
