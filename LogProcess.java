import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

/**
 * Message conversion: The sended/received message format is as follows:
 * <p>
 * Site1"APPEND"1'3'4"Hello
 * </p>
 * <p>
 * Site2"READ"1'2'3
 * </p>
 * <p>
 * Site3"RELEASEAPPEND"1'2'3
 * </p>
 * <p>
 * Site4"RELEASEREAD"1'2'3
 * </p>
 * Acknowledgement: ack
 * <p>
 * The output of log is as follows:
 * </p>
 * <p>
 * Site1 exclusive lock quorum 1, 3 and 4
 * </p>
 *
 */
public class LogProcess extends Thread {
	/**
	 * Server Port
	 */
	private final int port;

	/**
	 * Log data structure.
	 */
	private final ArrayList<String> messages;

	/**
	 * Server running flag.
	 */
	private boolean runningFlag;
	
	private InetAddress hostname;

	/**
	 * 
	 * Constructor of Log Process.
	 */
	LogProcess(InetAddress hostname, int port) {
		this.hostname = hostname;
		this.port = port;
		this.messages = new ArrayList<String>();
		this.runningFlag = true;

	}

	public static void main(String[] args) throws InterruptedException, IOException {
		ArrayList<String[]> config = readFile("config6.txt"); 
		InetAddress hostname = InetAddress.getByName(config.get(7)[0]);
		int port = Integer.parseInt(config.get(7)[1]);
		LogProcess logProcess = new LogProcess(hostname, port);
		logProcess.start();
		System.out.println("LogProcess is running on "+ config.get(7)[0] + " port: "+ config.get(7)[1]);
	}
	
	public static ArrayList<String[]> readFile(String fileName)
			throws IOException {
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		ArrayList<String[]> ProcessAddress = new ArrayList<String[]>();
		String[] ID = new String[1];
		ID[0] = bufferedReader.readLine();
		ProcessAddress.add(ID);
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			String[] addr = line.split(" ");
			ProcessAddress.add(addr);
		}
		bufferedReader.close();
		return ProcessAddress;
	}
	
	@Override
	public void run() {
		final ServerSocket serverSocket;
		try {
			serverSocket = new ServerSocket(port,5,hostname);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		while (runningFlag) {
			try {
				new ServerService(serverSocket.accept(), serverSocket).start();
			} catch (SocketException e) {
				return;
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}	
		}
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (String i : messages) {
			buffer.append(i);
			buffer.append("\'");
		}
		return buffer.toString();
	}

	private class ServerService extends Thread {
		private final Socket socket;
		private BufferedReader in;
		private PrintWriter out;
		private final ServerSocket serverSocket;

		public ServerService(Socket s, ServerSocket serverSocket) {
			socket = s;
			this.serverSocket = serverSocket;
			try {
				in = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				out = new PrintWriter(new BufferedWriter(
						new OutputStreamWriter(socket.getOutputStream())), true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			String event;
			try {
				while(true) {
					event = in.readLine();				
					if (processEvent(event) == false) {
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * Process the event
		 * 
		 * @param event
		 *            Event to be processed
		 * @return True if processed successfully. False otherwise.
		 */
		private boolean processEvent(String event) {
			String[] list = event.split("\"");
			String source = list[0];
			String operation = list[1];
			String[] quorum = list[2].split("\'");
			PrintEvent(source, operation, quorum);
			switch(operation) {
			case "APPEND": 
				{String msg = list[3];
				return processAppend(msg);} 
			case "READ": 
				return processRead();
			case "RELEASEAPPEND": 
				return processRelease();
			case "RELEASEREAD": 
				return processRelease();
			default:
				return false;
			}
		}

		/**
		 * Read the log content.
		 * 
		 * @return Log content.
		 */
		private boolean processRead() {
			String msg = LogProcess.this.toString();
			out.println(msg);
			return true;
		}

		/**
		 * Process append event.
		 * 
		 * @param msg
		 *            The message to be appended.
		 */
		private boolean processAppend(String msg) {
			messages.add(msg);
			out.println("ack");
			return true;
		}

		/**
		 * Process release event.
		 */
		private boolean processRelease() {
			out.println("ack");
			return false;
		}

		/**
		 * Print the event.
		 * 
		 * @param source
		 *            Source of the event.
		 * @param operation
		 *            Operation of the event.
		 * @param quorum
		 *            Quorum members.
		 */
		private void PrintEvent(String source, String operation, String[] quorum) {
			StringBuffer buffer = new StringBuffer();
			buffer.append(source);
			buffer.append(" ");
			switch (operation) {
			case "APPEND":
				buffer.append("exclusive lock");
				break;
			case "READ":
				buffer.append("shared lock");
				break;
			case "RELEASEAPPEND":
				buffer.append("release exclusive lock");
				break;
			case "RELEASEREAD":
				buffer.append("release shared lock");
				break;
			default:
				;
			}
			buffer.append(" ");
			buffer.append("quorum");
			buffer.append(" ");
			buffer.append(quorum[0]);
			buffer.append(", ");
			buffer.append(quorum[1]);
			buffer.append(" ");
			buffer.append("and ");
			buffer.append(quorum[2]);
			System.out.println(buffer.toString());
		}
	}
}
