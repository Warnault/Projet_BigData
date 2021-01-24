import org.apache.hadoop.util.ProgramDriver;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("convert",  data.convertNljsonToJson.class, "c'est d'la merde");
			pgd.addClass("TopsUser",  data.convertNljsonToJson.class, "c'est d'la merde");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}