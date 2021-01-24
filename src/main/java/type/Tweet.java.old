package type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.hadoop.io.Writable;

public class Tweet implements Writable, Cloneable {

  String _id;
  String _created_at;
  String _message;
  int _user_id;
  int _nb_retweet;
  ArrayList<String> _hashtags;
  String _language;
  String _country;
  public Tweet() {
  }

  public Tweet(String created_at, String text, int user_id, int nb_retweet, ArrayList<String> hashtags, String language, String country ) {
    this._created_at = new String(created_at);
    this._message = text;
    this._user_id = user_id;
    this._nb_retweet = nb_retweet;
    this._country = new String(country);
    this._language = new String(language);
    this._hashtags = new ArrayList<String>(hashtags);
  }

  public Tweet clone() {
    try {
      return (Tweet) super.clone();
    } catch (Exception e) {
      System.err.println(e);
      System.exit(-1);
    }
    return null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(_created_at);
    out.writeUTF(_message);
    out.writeUTF(_language);
    out.writeUFT(_country);
    out.writeInt(_user_id);
    out.writeInt(_nb_retweet);
    out.writeInt(_hashtags.size());
    for (String hashtag : _hashtags) {
      out.writeUTF(hashtag);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _created_at = in.readUTF();
    _message = in.readUTF();
    _user_id = in.readUTF();
    _language = in.readUTF();
    _country = in.readUTF();
    _nb_retweet = in.readInt();
    int hashtagCount = in.readInt();
    _hashtags = new ArrayList<String>();
    for (int i = 0; i < hashtagCount; i++) {
      _hashtags.add(in.readUTF());
    }
  }

  public String toString() {
    return( "Tweet" + _user_id);
  }

  public ArrayList<String> getHashTag() {
    return this._hashtags;
  }

  public ArrayList<String> getHastags(){return this.hashtags;}
  
  public String getCountry(){
    return this._country;
  }
  public String getLanguage(){
    return this._language;
  }
}
