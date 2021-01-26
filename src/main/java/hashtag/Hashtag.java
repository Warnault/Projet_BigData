package hashtag;

import org.apache.hadoop.util.ProgramDriver;

public class Hashtag {

	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("l", ListHashtag.class, "get list of hashtag"); 
			pgd.addClass("topk_freq", HashtagTopK.class, " get topk of hashtag, default k=10 or pass value of k in params");
			pgd.addClass("u", ListUserUseHashtag.class, "get list of user who use a hashtag passed as a params");
			pgd.addClass("f", HashtagFreq.class, "get freqence of a hashtag passed as a params");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
