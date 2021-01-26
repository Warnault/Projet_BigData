package user;

import org.apache.hadoop.util.ProgramDriver;

public class User {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("cpt_tweet", NbTweetForUser.class, "Get the number of tweet send by a user passes in parameter");
			pgd.addClass("l", TopLanguage.class, "Get list of languages used in Tweets");  
			pgd.addClass("c", TopCountry.class, "Get the list of countries where tweets come from "); 
			pgd.addClass("h", ListHashtagForAUser.class,"Get the list of hashtag used by a user passes in parameter");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
