public import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
  public static class TP3Mapper extends Mapper<Object, Text, Text, IntWritable>{
          public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String str = value.toString();
        final String tokens[] = str.split(",");
        context.getCounter("Stat", "nombre de ligne").increment(1);
        try {
          int nb_population = Integer.parseInt(tokens[4]);
          String keY = tokens[0]+","+tokens[1]+","+tokens[3];
          context.write(new Text(keY),new IntWritable(nb_population));
          context.getCounter("Stat", "nombre de ville").increment(1);
          context.getCounter("Stat", "population").increment(Integer.parseInt(tokens[4]));
        } catch (final Exception e) {}
      }
    }

    public static class TP3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
        int max = 0;
        for(IntWritable val : values)
          if( val.get()>max ) max = val.get();
        context.write(key, new IntWritable(max));
      }
    }

    public static void main(final String[] args) throws Exception {
      final Configuration conf = new Configuration();
      final Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


/* Value = {pays,ville,ville_acc,code_region,population,lat,lon} = {txt,txt,txt,int,double,double} 

0 1 3
*/