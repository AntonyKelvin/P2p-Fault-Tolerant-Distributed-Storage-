package Client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Client {
	
	static String ClientLocation;
	
	static String client_directory;
	
	public static boolean hostAvailabilityCheck(String ip_address,String port_numb)
	{
		Socket temp_socket = null;
		
		//PrintWriter temp_writer = null;
		
		//Not sending the client location makes the directory server work properly
		
		try
		{
			temp_socket = new Socket(ip_address,Integer.valueOf(port_numb));
			
			//temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
			
			//String send_to_server = 7 + ":" + ClientLocation ;
			
			//temp_writer.println(send_to_server);
			
			temp_socket.close();
			
			return true;
		}
		catch(Exception ex)
		{
			try
			{
				if(temp_socket != null)
				{
					temp_socket.close();
				}
				
			}
			catch(Exception ex1)
			{
				ex1.printStackTrace();
			}
		}
		
		return false;
	}

	public static boolean isPrimary(String ip_address,String port_numb) 
	{
		System.out.println("Address "+ip_address+" "+port_numb);
		
		Socket temp_socket = null;
		
		BufferedReader primaryReader = null;
		
		PrintWriter primaryWriter = null;
		
		try
		{
			temp_socket = new Socket(ip_address,Integer.valueOf(port_numb));
			
			primaryReader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
			
			primaryWriter = new PrintWriter(temp_socket.getOutputStream(),true);
			
			//Return from directory server to check if it is primary or not
			
			String send_to_directory_server = 2 + ":" + ClientLocation ;
			
			primaryWriter.println(send_to_directory_server);
			
			boolean primary = Boolean.valueOf(primaryReader.readLine()); 
			
			System.out.println("I am primary "+ primary);
			
			if(primary)
			{
				temp_socket.close();
				return true;
			}
			
		}
		catch(Exception ex)
		{
			try
			{
				temp_socket.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		if(temp_socket != null && !temp_socket.isClosed())
		{
			try
			{
				temp_socket.close();
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}

		
		return false;

	}
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		/*
		InetAddress localhost = InetAddress.getLocalHost(); 
        System.out.println("System IP Address : " + 
                      (localhost.getHostAddress()).trim());
		*/
		
		String[] directory_server_locations = {"localhost:4441" , "localhost:4442"};
		
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Welcome to our fault-tolerant distributed storage system");
		System.out.println("Enter your IP Address and PORT(IP:PORT)");
		
		ClientLocation = scan.next();
		
		System.out.println("Enter the directory location");
		
		client_directory = scan.next();
		
		
		int ch;
		
		
		
		while(true)
		{
			System.out.println("1.List the filenames");
			System.out.println("2.Request for a storage node");
			System.out.println("3.Exit");
			
			ch = scan.nextInt();
			
			if(ch == 1)
			{
				String primary_directory_server_location = "";
				for(String cur_location : directory_server_locations)
				{
					String[] ip_address = cur_location.split(":");
					if(hostAvailabilityCheck(ip_address[0],ip_address[1]))
					{
						System.out.println("Host Available " + ip_address[0]+":"+ip_address[1]);
						
						if(!isPrimary(ip_address[0],ip_address[1]))
						{
							continue;
						}
						else
						{
							primary_directory_server_location = primary_directory_server_location.concat(ip_address[0]+":"+ip_address[1]);
							break;
						}
						
					}
				}
				System.out.println("Primary server " + primary_directory_server_location);
				if(!primary_directory_server_location.equals(""))
				{
					//Create a temporary socket with the directory server and ask for the list of files
					long start = System.currentTimeMillis();
					
					Socket temp_socket = null;
					BufferedReader primaryServerReader = null;
					PrintWriter primaryServerWriter = null;
					try
					{
						String[] ip_address = primary_directory_server_location.split(":");
						
						temp_socket = new Socket(ip_address[0],Integer.valueOf(ip_address[1]));
						
						primaryServerReader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
						
						primaryServerWriter = new PrintWriter(temp_socket.getOutputStream(),true);
						
						String request_to_server = 3 + ":" + ClientLocation;
						
						primaryServerWriter.println(request_to_server);
						
						String[] from_server = primaryServerReader.readLine().split("~");
						
						System.out.println("List of files available");
						for(int i=0;i<from_server.length;i++)
						{
							System.out.println(from_server[i]);
						}
						
						long end = System.currentTimeMillis();
					    
					    float sec = (end - start) / 1000F; 
					    
					    System.out.println(sec + " seconds");
						
						temp_socket.close();
						
					}
					catch(Exception e)
					{
						try
						{
							temp_socket.close();
						}
						catch(Exception ex)
						{
							ex.printStackTrace();
						}
					}
				}
				else
				{
					System.out.println("Sorry none of the servers are currently active");
				}
	
			}
			else if(ch == 2)
			{
				//Request For a storage node
				String primary_directory_server_location = "";
				for(String cur_location : directory_server_locations)
				{
					String[] ip_address = cur_location.split(":");
					if(hostAvailabilityCheck(ip_address[0],ip_address[1]))
					{
						System.out.println("Host Available " + ip_address[0]+":"+ip_address[1]);
						
						if(!isPrimary(ip_address[0],ip_address[1]))
						{
							continue;
						}
						else
						{
							primary_directory_server_location = primary_directory_server_location.concat(ip_address[0]+":"+ip_address[1]);
							break;
						}
						
					}
				}
				String[] address = primary_directory_server_location.split(":");
				
				Socket temp_socket = null;
				
				PrintWriter temp_writer = null;
				
				BufferedReader temp_reader = null;
				
				try
				{
					/*
					temp_socket = new Socket(address[0],Integer.valueOf(address[1]));
					
					temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
					
					temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
					
					String send_to_server = 4 + ":" + ClientLocation;
					
					temp_writer.println(send_to_server);
					
					String storage_location = temp_reader.readLine();
					
					System.out.println("Storage Node location " + storage_location);
					*/
					
					//Make communication with the storage node
					while(true)
					{
						
						temp_socket = new Socket(address[0],Integer.valueOf(address[1]));
						
						temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
						
						temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
						
						String send_to_server = 4 + ":" + ClientLocation;
						
						temp_writer.println(send_to_server);
						
						String storage_location = temp_reader.readLine();
						
						System.out.println("Storage Node location " + storage_location);
						
						int choice;
						
						System.out.println("1.Read a file");
						System.out.println("2.Add a file to the storage node");
						System.out.println("3.Get the file list");
						System.out.println("4.Exit");
						choice = scan.nextInt();
						
						if(choice == 1)	
						{
							String file_name;
							
							System.out.println("Enter the filename you want to read");
							
							file_name = scan.next();
							
							String[] address_storage = storage_location.split(":");
							
							System.out.println("Address of storage " + address[0] + " " + address[1]);
							
							Socket temp_socket_storage = null;
							
							PrintWriter temp_socket_writer = null;
							
							BufferedReader temp_socket_reader = null;
							
							try
							{
								long start = System.currentTimeMillis();
								
								
								System.out.println("Address of storage " + address[0] + " " + address[1]);
								
								temp_socket_storage = new Socket(address_storage[0],Integer.valueOf(address_storage[1]));
								
								System.out.println("Socket established successfully");
								
								temp_socket_writer = new PrintWriter(temp_socket_storage.getOutputStream(),true);
								
								temp_socket_reader = new BufferedReader(new InputStreamReader(temp_socket_storage.getInputStream()));
								
								
								String to_server = 1 + ":" + ClientLocation + ":" + file_name;
								
								temp_socket_writer.println(to_server);
								
								String[] file_contents = temp_socket_reader.readLine().split("~");
								
								System.out.println("File contents are");
								for(int i=0;i<file_contents.length;i++)
								{
									System.out.println(file_contents[i]);
								}
							
								long end = System.currentTimeMillis();
							    
								
							    float sec = (end - start) / 1000F; 
							    
							    System.out.println(sec + " seconds");
								
								temp_socket_storage.close();
								
							}
							catch(Exception ex)
							{
								ex.printStackTrace();
							}
							
							if(temp_socket_storage != null && !temp_socket_storage.isClosed())
							{
								try
								{
									temp_socket_storage.close();
								}
								catch(Exception ex)
								{
									ex.printStackTrace();
								}
							}
							break;
						}
						else if(choice == 2)
						{
							String file_name;
							
							System.out.println("Enter the filename you wish to add");
							
							file_name = scan.next();
							
							//Ask for a storage node location before making a socket request
							
							
							String[] address_storage = storage_location.split(":");
							
							Socket temp_socket_storage = null;
							
							PrintWriter temp_socket_writer = null;
							
							BufferedReader temp_socket_reader = null;
							
							try
							{
								temp_socket_storage = new Socket(address_storage[0],Integer.valueOf(address_storage[1]));
								
								temp_socket_reader = new BufferedReader(new InputStreamReader(temp_socket_storage.getInputStream()));
								
								temp_socket_writer = new PrintWriter(temp_socket_storage.getOutputStream(),true);
								
								
								long start = System.currentTimeMillis();
								
								String to_server = 2 + ":" + ClientLocation + ":" + file_name;
								
								//Send the file content
								
								temp_socket_writer.println(to_server);
								
								String file_contents = "";
								
								File f = new File(client_directory+"\\"+file_name);
								
								BufferedReader reader = new BufferedReader(new FileReader(f));
								
								String line = "";
								
								while( (line = reader.readLine()) != null)
								{
									file_contents = file_contents.concat(line);
									file_contents = file_contents.concat("~");
								}
								
								temp_socket_writer.println(file_contents);
								
								
								String[] client_address = ClientLocation.split(":");
								ServerSocket server_socket = new ServerSocket(Integer.valueOf(client_address[1]));
								
								Socket client_socket = null;
								
								BufferedReader client_reader = null;
								
								try
								{
									client_socket = server_socket.accept();
									
									client_reader = new BufferedReader(new InputStreamReader(client_socket.getInputStream()));
									
									String status = client_reader.readLine();
									
									System.out.println("STATUS " + status);
									
									if(status.equals("SUCCESS"))
									{
										System.out.println("File successfully updated");
									}
									else
									{
										System.out.println("Sorry something went wrong. Please try again!");
										
										long end = System.currentTimeMillis();
										
										float sec = (end - start) / 1000F; 
										
										System.out.println(sec + " seconds");
										break;
									}
									
									server_socket.close();
									
									client_socket.close();
								}
								catch(Exception ex)
								{
									ex.printStackTrace();
								}
								
								long end = System.currentTimeMillis();
								
								float sec = (end - start) / 1000F; 
								
								System.out.println(sec + " seconds");
								
								if(server_socket != null && !server_socket.isClosed())
								{
									try
									{
										server_socket.close();
									}
									catch(Exception ex)
									{
										ex.printStackTrace();
									}
									
								}
								
								if(client_socket != null && !client_socket.isClosed())
								{
									try
									{
										client_socket.close();
									}
									catch(Exception ex)
									{
										ex.printStackTrace();
									}
								}
								
								
							}
							catch(Exception ex)
							{
								ex.printStackTrace();
							}
							break;
						}
						else if(choice == 3)
						{
							Socket temp_storage_socket = null;
							
							BufferedReader temp_storage_reader = null;
							
							PrintWriter temp_storage_writer = null;
							
							System.out.println("STORAGE NODE LOCATION " + storage_location);
							
							String[] address_storage = storage_location.split(":");
							
							try
							{
								temp_storage_socket = new Socket(address_storage[0],Integer.valueOf(address_storage[1]));
								
								temp_storage_writer = new PrintWriter(temp_storage_socket.getOutputStream(),true);
								
								temp_storage_reader = new BufferedReader(new InputStreamReader(temp_storage_socket.getInputStream()));
								
								
								String to_server = 3 + ":" + ClientLocation + ":" + "list_of_files.txt";
								
								temp_storage_writer.println(to_server);
								
								String[] file_contents = temp_storage_reader.readLine().split("~");
								
								System.out.println("List of files");
								for(int i=0;i<file_contents.length;i++)
								{
									System.out.println(file_contents[i]);
								}
								
							}
							catch(Exception ex)
							{
								ex.printStackTrace();
							}
							break;
						}
						else
						{
							break;
						}
						
					}
					
					temp_socket.close();
					
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
				
			}
			else
			{
				break;
			}
			
		}

	}

}