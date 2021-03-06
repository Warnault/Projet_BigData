package hashtag;

import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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


public class ListUserUseHashtag {

  public static class ListUserUseHashtagMapper extends Mapper<Object, Text, Text, IntWritable>{
    @Override
    public void map(Object key, Text value, Context context )throws IOException,InterruptedException {
      JsonNode json = new ObjectMapper().readTree(value.toString());
      if( json.has("delete")) return;
      if (json.hasNonNull("user")){
        String _hashtag = context.getConfiguration().get("hashtag");
        ArrayList<String> hashtag = new ArrayList<>( json.get("entities").get("hashtags").findValuesAsText("text") );
        for( String h : hashtag )
          if( h.equals(_hashtag) )
            context.write(new Text (json.get("user").get("screen_name").asText()), new IntWritable(1));   
      }
    }
  }

  public static class ListUserUseHashtagCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for( IntWritable v : values){
        sum += v.get();
      }
      context.write( key, new IntWritable(sum) );
    }
  } 

  public static class ListUserUseHashtagReducer extends Reducer<Text,IntWritable,Text,NullWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
      int sum = 0;
      for( IntWritable v : values)
        sum += v.get();
      context.write( key, NullWritable.get() );
    }
  }

      
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String _hashtag = args[0];
    conf.set("hashtag", _hashtag);

    Job job = Job.getInstance(conf, "Main");
		job.setNumReduceTasks(1);
		job.setJarByClass(ListUserUseHashtag.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		//Mapper
    job.setMapperClass(ListUserUseHashtagMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //Combiner
		job.setCombinerClass(ListUserUseHashtagCombiner.class);


		//Reducer
		job.setReducerClass(ListUserUseHashtagReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
