package users;

import org.apache.hadoop.util.ProgramDriver;

public class Users {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("nb_tweets", NbTweetForUser.class, "nb tweet for user");
			pgd.addClass("language", TopLanguage.class, "top language");
			pgd.addClass("country", TopCourntry.class, "top country");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
