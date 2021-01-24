
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import type.Tweet;
import data.convertNljsonToJson.ConvertMapper;

public class TopCountry {

  public class TopCountryCombiner extends Reducer(Null\,Tweet,Text,IntWritable){
    public void reduce(NullWritable key, Iterable<Tweet> values, Context context) throws IOException, InterruptedException{
      for( Tweet t : value)
      context.write( new Text(t.getCountry()), IntWritable(1) );
    }
  }
  
  public class TopCountryReducer extends Reducer(Text,IntWritable,Text,IntWritable){
    public void reduce(Text key, Iterable<IntWritable> value, Context context){
      int sum = 0;
      for( IntWritable i : value){
        sum++;
        context.write(key, IntWritable(sum));
      }
    }
  }
     
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Main");
		job.setNumReduceTasks(1);
		job.setJarByClass(TopCountry.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));

		//Mapper
    job.setMapperClass(ConvertMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Tweet.class);
        
		//Combiner
		job.setCombinerClass(TopCountryCombiner.class);

		//Reducer
		job.setReducerClass(TopCountryReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
