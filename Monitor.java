
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;


 
public class Monitor {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws Exception{
		File homedir = new File(System.getProperty("user.home"));
		File file = new File(homedir, "/supercool.txt");
		FileWriter fr = null;
		fr = new FileWriter(file);
		for (int i = 0; i < 10; i++) {
			fr.write(Long.toString(System.currentTimeMillis()) + "\n");
	        	fr.write("Used Memory   :  " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " bytes\n");
	        	fr.write("Free Memory   : " + Runtime.getRuntime().freeMemory() + " bytes\n");
	        	fr.write("Total Memory  : " + Runtime.getRuntime().totalMemory() + " bytes\n");
	        	fr.write("Max Memory    : " + Runtime.getRuntime().maxMemory() + " bytes\n\n");     
	        	Thread.sleep(4000);
		}
		fr.close();
   
	}

}
