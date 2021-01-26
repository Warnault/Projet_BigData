package user;

import org.apache.hadoop.util.ProgramDriver;

public class TopsUser {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("cpt_tweet", NbTweetForUser.class, ""); // nb tweet for a user
			pgd.addClass("l", TopLanguage.class, "");  // list language
			pgd.addClass("c", TopCountry.class, ""); // list country
			pgd.addClass("h", ListHashtagForAUser.class,""); // list hastag for a user
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
