import org.apache.hadoop.util.ProgramDriver;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("User",  user.User.class, "search in User");
			pgd.addClass("Hashtag",  hashtag.Hashtag.class, "search in Hashtag");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}