package hashtag;

import org.apache.hadoop.util.ProgramDriver;

public class Hashtag {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("l", ListHashtag.class, ""); 
			pgd.addClass("topk", HashtagTopK.class, "");
			pgd.addClass("topk_freq", HashtagTopK.class, "");
			pgd.addClass("u", ListUserUseHashtag.class, "");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
