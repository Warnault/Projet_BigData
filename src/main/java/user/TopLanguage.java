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

public class TopLanguage {

  public static class TopLanguageMapper extends Mapper<Object,Text,Text,IntWritable>{

    @Override
    public void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
      
      JsonNode json = new ObjectMapper().readTree(value.toString());
      if (json.has("delete")) {
        return;
      }
      String lang = json.get("lang").asText();
      context.write(new Text(lang), new IntWritable(1));
    }
  }

  public static class TopLanguageCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{

    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for( IntWritable v : value)
        sum += v.get();
      context.write(key, new IntWritable(sum));
    }
  }

  public static class TopLanguageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
		  for (IntWritable val : values)
        sum += val.get();
      context.write(key, new IntWritable(sum));
    }
	}
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Main");
    job.setNumReduceTasks(1);
    
		job.setJarByClass(TopLanguage.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));

		//Mapper
    job.setMapperClass(TopLanguageMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
		//Combiner
		job.setCombinerClass(TopLanguageCombiner.class);

		//Reducer
		job.setReducerClass(TopLanguageReducer.class);
    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);		
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
