import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class Monitor {
	public static void main(String[] args) throws Exception{
		File homedir = new File(System.getProperty("user.home"));
		File file = new File(homedir, "/signal.txt");
		while (true) {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String str = br.readLine();
			br.close();
			// If the job hasn't started.
	        if (str == null || str.charAt(0) == '-') {
	        	Thread.sleep(1300);
	        	continue;
	        }
	        // Detect start signal, start monitoring.
			if (str.charAt(0) == '+') monitor(100);
			// Detect exit signal, exit.
			if (str.charAt(0) == '!') break;
		}
		System.out.println("monitor exit.");

	}
	public static void monitor(int period) throws Exception {
		// Call garbage collection explicitly.
		System.gc();
		long idlemem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long maxmem = 0;
		while (true) {
			long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			if (maxmem < mem) maxmem = mem;  
	        Thread.sleep(period);
	        if (jobEnd()) break;
		}
		wrt_mem_log((maxmem - idlemem) + "\n");
	}
	
	public static boolean jobEnd() throws Exception{
		File homedir = new File(System.getProperty("user.home"));
		File file = new File(homedir, "/signal.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String str = br.readLine();
		br.close();
		if (str.charAt(0) == '-') {
			wrt_mem_log(str + ": ");
			return true;
		}

		return false;
	}
	
	public static void wrt_mem_log(String str) throws IOException {
		File homedir = new File(System.getProperty("user.home"));
		File file = new File(homedir, "/mem_log.txt");
		FileWriter fr = null;
		BufferedWriter br = null;
		fr = new FileWriter(file, true);
		br = new BufferedWriter(fr);
		br.append(str);
		br.close();
		fr.close();
	}

}
