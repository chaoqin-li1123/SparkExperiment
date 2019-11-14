
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
		long idlemem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		int size = 124;
		while (size < 1000) {
			size *= 2;
			accuracy(size, idlemem);
			Thread.sleep(5000);
		}   
	}
	public static void accuracy(int size, long idlemem) throws Exception {
		int[][][] array = new int[size][256][1024];
		long maxmem = 0;
		for (int time = 0; time < 5; time++) {
			long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			if (maxmem < mem) maxmem = mem;  
			array[0][1][0] = 5;
	        Thread.sleep(400);
		}
		write(maxmem, size, idlemem);
		array = null;
	}
	public static void write(long mem, int expected, long idlemem) throws Exception{
		mem -= idlemem;
		mem /= (1024 * 1024);
		File homedir = new File(System.getProperty("user.home"));
		File file = new File(homedir, "/supercool.txt");
		FileWriter fr = null;
		BufferedWriter br = null;
		fr = new FileWriter(file, true);
		br = new BufferedWriter(fr);
		String str = "Expected : " + expected + " MB,   ";
		str += "Actual : " + mem + " MB\n";
		br.append(str);
		br.close();
		fr.close();		
	}

}
