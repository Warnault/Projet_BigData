import java.io.IOException;

import javax.naming.Context;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopHashtags  extends Configured implements Tools{

	public static class HashTags implements Writable {
		public String _name;
		public int _cpt;
				
		public HashTags(String name, int cpt) {
			this._name = name;
			this._cpt = cpt;
		}
		
		public void readFields(DataInput in) throws IOException {
			this._name = in.readUTF();
			this._cpt = in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(_name);
			out.writeInt(_cpt);
		}
	}
  


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

    public static class SearchHasthtag extends Reducer<NullWritable,Tweet,NullWritable,HashTags> {
      @Override
      private void reduce(NullWritable key, Iterable<Tweet> values, Context context ){
        for( Tweet t : values){
          ArrayList<String> hashtag = values.getHashTag();
          for( String h : hashtag)
            context.write(NullWritable.get(), new HashTags(h, 1));
        }
      }
    }
  
    public static class TopKHashtagCombiner extends Reducer<NullWritable, CityPop, NullWritable, CityPop> {
      private int k = 10;
      
      @Override
      protected void setup(Reducer<NullWritable, Tweet, NullWritable, HashTags>.Context context) throws IOException, InterruptedException {
        this.k = context.getConfiguration().getInt("k", 10);
      }
  
      @Override
      protected void reduce(NullWritable key, Iterable<Tweet> values, Reducer<NullWritable, HashTags, NullWritable, HashTags>, Context context) throws IOException, InterruptedException {
        TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
        for(CityPop cp : values) {
          //insertion de la ville
          topk.put(cp.pop, cp.name);
          // on conserve les k plus grandes
          while(topk.size() > k) {
            topk.remove(topk.firstKey());
          }
        }
        
        // ecrire les k plus grandes
        for (Map.Entry<Integer, String> v : topk.entrySet()) {
          context.write(NullWritable.get(), new CityPop(v.getValue(), v.getKey()));
        }
      }
    }
  }
}
