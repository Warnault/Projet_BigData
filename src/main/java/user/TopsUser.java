package user;

import org.apache.hadoop.util.ProgramDriver;

public class TopsUser {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			//pgd.addClass("nb_tweets", NbTweetForUser.class, "nb tweet for user");
			pgd.addClass("l", TopLanguage.class, "");
			pgd.addClass("c", TopCountry.class, "");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}