package user;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NbTweetForUser {

  public static class NbTweetForUserMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
      String _screen_name = context.getConfiguration().get("screen_name");
      JsonNode json = new ObjectMapper().readTree(value.toString());
      if( json.has("delete") ) return;     
      if(json.hasNonNull("user")){
        String screen_name = json.get("user").get("screen_name").asText();
        if( screen_name.equals(_screen_name))
          context.write(new Text(screen_name), new IntWritable(1));
      }
    }
  }
  /*
  public static class NbTweetForUserCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for( IntWritable v : values){
        sum ++;
        context.write( key, new IntWritable(sum) );
      }
    }
  }
    */ 
  
  public static class NbTweetForUserReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException,InterruptedException {
      int sum = 0;
      for( IntWritable v : value){
        sum ++;
      }
      context.write(key, new IntWritable(sum));
    }
  }
     
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String screen_name = args[0];
    conf.set("screen_name",screen_name);

    Job job = Job.getInstance(conf, "Main");
		job.setNumReduceTasks(1);
		job.setJarByClass(TopCountry.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		//Mapper
    job.setMapperClass(NbTweetForUserMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //Combiner
		//job.setCombinerClass(NbTweetForUserCombiner.class);


		//Reducer
		job.setReducerClass(NbTweetForUserReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
