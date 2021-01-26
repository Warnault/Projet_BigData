import org.apache.hadoop.util.ProgramDriver;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("topsUser",  user.TopsUser.class, "");
			pgd.addClass("Hashtag",  hashtag.Hashtag.class, "c'est d'la merde");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}