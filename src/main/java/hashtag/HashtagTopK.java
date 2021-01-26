package hashtag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import javax.security.auth.login.ConfigurationSpi;

import java.util.Map;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import hashtag.HashtagTopK.Hashtag;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import hashtag.ListHashtag;

public class HashtagTopK extends Configured {
  
  public static class Hashtag implements Writable {
    
    public String hashtag_name;
    public int freq;
    
    public Hashtag() {
      this.hashtag_name = "";
      this.freq = 0;
    }
    
    public Hashtag(String hashtag_name, int freq) {
      this.hashtag_name = hashtag_name;
      this.freq = freq;
    }
    
    public void readFields(DataInput in) throws IOException {
      this.hashtag_name = in.readUTF();
      this.freq = in.readInt();
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeUTF(hashtag_name);
      out.writeInt(freq);
    }
    
  }

  //Mapper
  public static class HashtagTopKMapper extends Mapper<LongWritable, Text, NullWritable, Hashtag> {

    private TreeMap<Integer,String> topk = new TreeMap<Integer,String>();

    protected void map(LongWritable key, Text value,  Mapper<LongWritable, Text, NullWritable, Hashtag>.Context context) throws IOException, InterruptedException {

      String[] s = value.toString().split("\\s+");
      topk.put(Integer.valueOf(s[1]),s[0]);   
      //int k = context.getConfiguration().getInt("k",8);
      int k = 10;
      while (topk.size() > k)
        topk.remove(topk.firstKey());
    }

    protected void cleanup(Mapper<LongWritable, Text, NullWritable, Hashtag>.Context context)	throws IOException, InterruptedException {
      Hashtag has = new Hashtag();
      for(Map.Entry<Integer,String> pair : topk.entrySet()) {
        has.hashtag_name = pair.getValue();
        has.freq = pair.getKey();
        context.write(NullWritable.get(), has);
      }
    }
  }
  
  //Combiner
  public static class HashtagTopKCombiner extends Reducer<NullWritable, Hashtag, NullWritable, Hashtag> {
    @Override
    protected void reduce(NullWritable key, Iterable<Hashtag> values,  Reducer<NullWritable, Hashtag, NullWritable, Hashtag>.Context context) throws IOException, InterruptedException {
      //int k = context.getConfiguration().getInt("k",8);
      int k=10;
      TreeMap<Integer,String> topk = new TreeMap<Integer,String>();
      for(Hashtag h : values) {
        topk.put(h.freq,h.hashtag_name);
        while(topk.size() > k) 
          topk.remove(topk.firstKey());
      }
      for (Map.Entry<Integer,String> v : topk.entrySet())
        context.write(NullWritable.get(), new Hashtag(v.getValue(),v.getKey() ));      
    }
  }
  
  //Reducer
  public static class HashtagTopKReducer extends Reducer<NullWritable, Hashtag, Text, IntWritable> {
 
    @Override
    protected void reduce(NullWritable key, Iterable<Hashtag> values, Reducer<NullWritable, Hashtag, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      
      TreeMap<Integer,String> topk = new TreeMap<Integer,String>();
      //int k = context.getConfiguration().getInt("k",8);
      int k=10;
      for(Hashtag h : values) {
        topk.put( h.freq,h.hashtag_name);
        while(topk.size() > k)
          topk.remove(topk.firstKey());
      }
      for (Map.Entry<Integer,String> v : topk.entrySet()) 
        context.write(new Text(v.getValue()), new IntWritable(v.getKey()) );
    }
  }


  public static void main(String[] args) throws Exception {
    //////// JOB 1 ////////////
    Configuration conf1  =new Configuration();

    Path save_data = new Path("saveData"); 

    Job job1 = Job.getInstance(conf1, "Main1");
		job1.setNumReduceTasks(1);
		job1.setJarByClass(ListHashtag.class);

		job1.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job1, new Path(args[1]));

		//Mapper
    job1.setMapperClass(ListHashtag.ListHashtagMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);

    //Combiner
		job1.setCombinerClass(ListHashtag.ListHashtagCombiner.class);

		//Reducer
		job1.setReducerClass(ListHashtag.ListHashtagReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    FileOutputFormat.setOutputPath(job1, save_data); 
    job1.waitForCompletion(true); 
    ////////////////////////////
    
    //////// JOB 2 ////////////
    Configuration conf = new Configuration();
    int k = 0;
    k = Integer.parseInt(args[0]);
    conf.setInt("k", k);
    
    Job job2 = Job.getInstance(conf, "Main2");
    job2.setNumReduceTasks(1);
    job2.setJarByClass(HashtagTopK.class);
    
    job2.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job2, save_data);

    //Mapper
    job2.setMapperClass(HashtagTopKMapper.class);
    job2.setMapOutputKeyClass(NullWritable.class);
    job2.setMapOutputValueClass(Hashtag.class);

    //Combiner
    job2.setCombinerClass(HashtagTopKCombiner.class);

    //Reducer
    job2.setReducerClass(HashtagTopKReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    ////////////////////////////

    TextOutputFormat.setOutputPath(job2, new Path(args[2]));
  
    System.exit( job2.waitForCompletion(true) ? 0 : 1);
  }
  
}