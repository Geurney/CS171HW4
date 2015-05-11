import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


public class Site {
	private CLIThread CLI;
	private CommThread Comm;
	private ArrayList<String[]> config;
	private int siteID;

	public Site(ArrayList<String[]> config) {
		this.config = config;
		CLI = new CLIThread();
		try {
			InetAddress hostname = InetAddress.getByName(this.config.get(7)[0]);
			int port = Integer.parseInt(this.config.get(7)[1]);
			Comm = new CommThread(hostname, port);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.siteID = Integer.parseInt(this.config.get(0)[0]);
		CLI.start();
		Comm.start();
	}

	private class CLIThread extends Thread {
		Scanner sc = new Scanner(System.in);

		@Override
		public void run() {
			String command;
			while ((command = sc.next()) != null) {
				Random rand = new Random();
				int[] Q = new int[2];
				HashSet<Integer> quorum = new HashSet<Integer>();
				quorum.add(siteID);
				for (int i = 0; i < 2; i++) {
					Q[i] = rand.nextInt(5) + 1;
					while (quorum.contains(Q[i]))
						Q[i] = rand.nextInt(5) + 1;
					quorum.add(Q[i]);
				}
				while (true) {
					sendRequest(Q[0], Q[1], command);
					// accept grant or fail message.
					ServerSocket serverSocket;
					try {
						InetAddress hostname = InetAddress.getByName(config
								.get(siteID)[0]);
						int port = Integer.parseInt(config.get(siteID)[1]);
						serverSocket = new ServerSocket(port + 1, 5, hostname);
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}
					List<String> result = new ArrayList<String>();
						Socket mysocket;
						try {
							// Wait for a client to connect (blocking)
							mysocket = serverSocket.accept();
						} catch (IOException e) {
							e.printStackTrace();
							continue;
						}
						BufferedReader in;
						try {
							in = new BufferedReader(new InputStreamReader(
									mysocket.getInputStream()));
						} catch (IOException e) {
							e.printStackTrace();
							continue;
						}

						// System.out.println("Server: Established connection with a client");

						// Read event from client
						String input;
						String[] input_split = null;
						try {
							input = in.readLine();
							input_split = input.split("\"");
						} catch (IOException e) {
							e.printStackTrace();
							input = "UnknownEvent";
						}

						// System.out.println("Server: Received Event " +
						// input);
						String operation = input_split[1];
						switch (operation) {
						case "FAIL":
							result.add(input);
							break;

						case "GRANT":
							result.add(input);
							break;
						}
					}
				}
			}
		}

		private void accessLog(int Q1, int Q2, String command) {
			Socket mysocket;
			try {
				mysocket = new Socket(config.get(6)[0], Integer.parseInt(config
						.get(6)[1]));
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			// Establish input and output streams with the server
			PrintWriter out;
			BufferedReader in;
			try {
				out = new PrintWriter(mysocket.getOutputStream(), true);
				in = new BufferedReader(new InputStreamReader(
						mysocket.getInputStream()));
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			// Send event to log
			StringBuilder sb = new StringBuilder();
			String operation = command.substring(0, command.indexOf(' '));
			sb.append("Site").append(siteID).append("\"").append(operation)
					.append("\"").append(siteID).append("\'").append(Q1)
					.append("\'").append(Q2).append("\"");
			if (operation.startsWith("A"))
				sb.append(command.substring(command.indexOf(' ') + 1,
						command.length()));
			out.println(sb.toString());
			String reply = null;
			if (command.charAt(0) == 'R') {
				// Wait for a reply from the log (blocking)
				try {
					reply = in.readLine();
					System.out.println("Read from the log: " + reply);
					out.println("Site" + siteID + "\"RELEASEREAD\"" + siteID
							+ "\'" + Q1 + "\'" + Q2);
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
			} else {
				// Wait for a reply from the log (blocking)
				try {
					reply = in.readLine();
					System.out
							.println("Already append the message to the log!");
					out.println("Site" + siteID + "\"RELEASEAPPEND\"" + siteID
							+ "\'" + Q1 + "\'" + Q2);
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
			}
			try {
				reply = in.readLine();
				System.out.println("Receive the acknowledge from the log!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// Close TCP connection
			try {
				mysocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		private void sendRelease(int q) {
			Socket mysocket;
			try {
				mysocket = new Socket(config.get(q)[0], Integer.parseInt(config
						.get(q)[1]));
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			PrintWriter out;
			try {
				out = new PrintWriter(mysocket.getOutputStream(), true);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			// Send event to server
			out.println(siteID + "\"" + "RELEASE");

			// Close TCP connection
			try {
				mysocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		private void sendRequest(int Q1, int Q2, String command) {
			Socket mysocket1;
			Socket mysocket2;
			Socket mysocket3;
			try {
				mysocket1 = new Socket(config.get(siteID)[0],
						Integer.parseInt(config.get(siteID)[1]));
				mysocket2 = new Socket(config.get(Q1)[0],
						Integer.parseInt(config.get(Q1)[1]));
				mysocket3 = new Socket(config.get(Q2)[0],
						Integer.parseInt(config.get(Q2)[1]));
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			// Establish input and output streams with the server
			PrintWriter out1;
			PrintWriter out2;
			PrintWriter out3;
			try {
				out1 = new PrintWriter(mysocket1.getOutputStream(), true);
				out2 = new PrintWriter(mysocket2.getOutputStream(), true);
				out3 = new PrintWriter(mysocket3.getOutputStream(), true);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			// Send event to server
			if (command.charAt(0) == 'R') {
				out1.println(siteID + "\"" + "READ");
				out2.println(siteID + "\"" + "READ");
				out3.println(siteID + "\"" + "READ");
			} else {
				out1.println(siteID + "\"" + "WRITE");
				out2.println(siteID + "\"" + "WRITE");
				out3.println(siteID + "\"" + "WRITE");
			}

			// Close TCP connection
			try {
				mysocket1.close();
				mysocket2.close();
				mysocket3.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private class CommThread extends Thread {
		private int port;
		private List<String> activeLock;
		private List<String> requestLock;
		private InetAddress privateIP;

		public CommThread(InetAddress privateIP, int port) {
			this.port = port;
			this.activeLock = new ArrayList<String>();
			this.requestLock = new ArrayList<String>();
			this.privateIP = privateIP;
		}

		@Override
		public void run() {
			ServerSocket serverSocket;
			try {
				serverSocket = new ServerSocket(port, 5, privateIP);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			while (true) {
				Socket mysocket;
				try {
					// Wait for a client to connect (blocking)
					mysocket = serverSocket.accept();
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
				// Establish input and output streams with the client
				PrintWriter out;
				BufferedReader in;
				try {
					out = new PrintWriter(mysocket.getOutputStream(), true);
					in = new BufferedReader(new InputStreamReader(
							mysocket.getInputStream()));
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}

				// System.out.println("Server: Established connection with a client");

				// Read event from client
				String input;
				String[] input_split = null;
				try {
					input = in.readLine();
					input_split = input.split("\"");
				} catch (IOException e) {
					e.printStackTrace();
					input = "UnknownEvent";
				}

				// System.out.println("Server: Received Event " + input);
				String site = input_split[0];
				String operation = input_split[1];
				switch (operation) {
				case "RELEASE":
					// Remove the request from the list of active locks
					for (Iterator<String> it = activeLock.iterator(); it
							.hasNext();) {
						String val = it.next();
						if (val.contains(site)) {
							it.remove();
							// jump out of the loop
							break;
						}
					}
					// Check if this lock-release permits other requests from
					// the queue to be granted
					if (requestLock.isEmpty() != true) {
						if (activeLock.isEmpty()) {
							String newRequest = requestLock.remove(0);
							activeLock.add(newRequest);
							sendGrant(newRequest.substring(0,
									newRequest.indexOf('\"')));
						}
					}
					break;

				case "READ":
					if (activeLock.get(0).contains("WRITE"))
						SendFail(site);
					else {
						activeLock.add(input);
						sendGrant(site);
					}
					break;

				case "WRITE":
					if (activeLock.isEmpty()) {
						activeLock.add(input);
						sendGrant(site);
					} else {
							SendFail(site);
							requestLock.add(input);
						}
					}
					break;


				}

			}
		}

		private void SendFail(String site) {
			Socket mysocket;
			try {
				mysocket = new Socket(config.get(Integer.parseInt(site))[0],
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			PrintWriter out;
			try {
				out = new PrintWriter(mysocket.getOutputStream(), true);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			out.println(siteID + "\"" + "FAIL");
			// Close TCP connection
			try {
				mysocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		private void sendGrant(String site) {
			Socket mysocket;
			try {
				mysocket = new Socket(config.get(Integer.parseInt(site))[0],
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			PrintWriter out;
			try {
				out = new PrintWriter(mysocket.getOutputStream(), true);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			out.println(siteID + "\"" + "GRANT");
			// Close TCP connection
			try {
				mysocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

}
