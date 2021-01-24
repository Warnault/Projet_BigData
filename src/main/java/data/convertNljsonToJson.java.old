package data;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import type.Tweet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;

public class convertNljsonToJson {
  
  public static class ConvertMapper extends Mapper<Object, Text, NullWritable, Tweet>{
    
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
      final String data = value.toString();
      context.getCounter("Stat", "nombre de ligne").increment(1);
      try {  
        JsonParser json = new JsonParser();
        Object o = json.parse(data);
        JsonObject objet_json = (JsonObject) o;

        int id_tweet = objet_json.get("id").getAsInt();
        String created_at = objet_json.get("create_ad").getAsString();
        String message = objet_json.get("text").getAsString();
        int user_id = objet_json.getAsJsonObject("user").get("id").getAsInt();
        int nb_retweet = objet_json.get("retweet_count").getAsInt();

        ArrayList<String> hashtags = new ArrayList<String>();
        JsonArray jsonHashtags = objet_json.getAsJsonObject("entities").getAsJsonArray("hashtags");
        for (int i = 0; i < jsonHashtags.size(); i++) {
          hashtags.add(jsonHashtags.get(i).getAsJsonObject().get("text").getAsString());
        }

        objet_json.getAsJsonObject("entities").getAsJsonArray("hashtags");
  
        Tweet tweet = new Tweet(created_at, message, user_id, nb_retweet, hashtags);  
        context.getCounter("Stat", "nombre de Tweet").increment(1);

        context.write( NullWritable.get(), tweet);
      } catch (final Exception e){
        System.out.println(e);
      }
    }

  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "TwitterProject");
        job.setNumReduceTasks(1);

        job.setJarByClass(convertNljsonToJson.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[1]));

        //Mapper
    job.setMapperClass(ConvertMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Tweet.class);

    job.setOutputFormatClass(TextOutputFormat.class);        
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}