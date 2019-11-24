package Directory_Server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;

public class DirectoryServer_1 {
	
	
	public static String directory_server_location = "C:\\Users\\antof\\Desktop\\Directory_Server\\DS1";
	
	static String[] other_directory_server_list = {"localhost:4442"};
	
	static boolean isPrimary = true;
	
	static String my_location = "localhost:4441"; 
	
	static ArrayList<String>storage_node_list = new ArrayList<String>();
	
	//hostAvailabilityCheck() - Function check if the ip address is currently running or not
	
	//Start of hostAvailabilityCheck()
	
	
	synchronized public static void delete_from_storage_list(String storage_node_location) throws IOException
	{
		
		System.out.println("Delete location " + storage_node_location);
		
		String[] final_file_contents = null;
		
		//if(storage_node_list.contains(storage_node_location))
		//{
			//storage_node_list.remove(storage_node_location);
			
			File f = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
			
			
			if(f.exists())
			{
				BufferedReader temp_reader = new BufferedReader(new FileReader(f));
				
				String line = "";
				
				String file_contents = "";
				
				while( (line = temp_reader.readLine()) != null)
				{
					if(!line.equals(storage_node_location))
					{
						System.out.println("Content " + line);
						
						file_contents = file_contents.concat(line);
						
						file_contents = file_contents.concat("~");
					}
				}
				
				final_file_contents = file_contents.split("~");
				
				
				temp_reader.close();
				
				try
				{
				if(f.delete())
				{
					System.out.println("File successfully deleted");
				}
				else
				{
					//THIS SHOULDNT HAPPEN
					System.out.println("File deletion unsuccessfull");
				}
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
			}
			else
			{
				
				
				System.out.println("THE STORAGE NODE FILE ITSELF DOESNT EXIST");
			}
			
			File storage_f = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
			
			if(storage_f.createNewFile())
			{
				PrintWriter write_file = new PrintWriter(storage_f);
				
				for(int i=0;i<final_file_contents.length;i++)
				{
					write_file.println(final_file_contents[i]);
				}
				
				write_file.close();
				
				System.out.println("CONTENTS WRITTEN TO STORAGE FILE SUCCESSFULLY");
			}
			
			//Delete the entry from temp file
			File f_temp = new File(DirectoryServer_1.directory_server_location+"\\"+"temp.txt");
			
			boolean isOneEntry = false;
			
			if(f_temp.exists())
			{
				BufferedReader temp_reader = new BufferedReader(new FileReader(f_temp));
				
				String line = "";
				
				String file_contents = "";
				
				while( (line = temp_reader.readLine()) != null)
				{
					if(!line.equals(storage_node_location))
					{
						
						System.out.println("Content " + line);
						
						isOneEntry = true;
						
						file_contents = file_contents.concat(line);
						
						file_contents = file_contents.concat("~");
					}
				}
				
				final_file_contents = file_contents.split("~");
				
				System.out.println("CONTENTS WRITTEN TO TEMP FILE SUCCESSFULLY");
				
				temp_reader.close();
				
				try
				{
				if(f_temp.delete())
				{
					System.out.println("File successfully deleted");
				}
				else
				{
					System.out.println("File deletion unsuccessfully");
				}
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
			}
			else
			{
				System.out.println("Storage node file exists");
			}
			
			if(isOneEntry)
			{
				File temp_f_add = new File(DirectoryServer_1.directory_server_location+"\\"+"temp.txt");
				
				if(temp_f_add.createNewFile())
				{
					PrintWriter write_file = new PrintWriter(temp_f_add);
					
					for(int i=0;i<final_file_contents.length;i++)
					{
						write_file.println(final_file_contents[i]);
					}
					
					write_file.close();
				}
				
			
			}
			
					
		//}
	}
	
	public static boolean hostAvailabilityCheck(String ip_address,String port_number)
	{
		Socket temp_socket = null;
		
		PrintWriter temp_writer = null;
		
		try
		{
			temp_socket = new Socket(ip_address,Integer.valueOf(port_number));
			
			//temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
			
			//String send_to_server = 7 + ":" + my_location ;
			
			//temp_writer.println(send_to_server);
			
			temp_socket.close();
			
			return true;
		}
		catch(Exception socket_exception)
		{
			//System.out.println("Server not available");
			//socket_exception.printStackTrace();
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
	
	//End of hostAvailabilityCheck()
	


	
	
	
	//isPrimaryServer() - Checks if the server running is primary or not
	
	//Start of isPrimaryServer()
	
	public static boolean isPrimaryServer(String ip_address,String port_number)
	{
		Socket temp_socket = null;
		
		BufferedReader temp_reader = null;
		
		PrintWriter temp_writer = null;
		
		try
		{
			temp_socket = new Socket(ip_address,Integer.valueOf(port_number));
			
			temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
			
			temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
			
			String send_to_server = 2 + ":" + my_location;
			
			temp_writer.println(send_to_server);
			
			boolean receive_from_server = Boolean.valueOf(temp_reader.readLine());
			
			if(receive_from_server)
			{
				temp_socket.close();
				
				return true;
			}
		}
		catch(Exception socket_exception)
		{
			socket_exception.printStackTrace();
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
	
	//End of isPrimaryServer()
	
	
	
	
	
	
	
	
	//preprocess() - Find the primary server running and recovers all the files from him

	//Start of preprocess() function
	
	public static void preprocess()
	{
		Socket primary_directory_server = null;
		
		BufferedReader primary_reader = null;
		
		PrintWriter primary_writer = null;
		
		String primary_location = "";
		
		//Finding the primary server starts
		
		for(String find_primary : other_directory_server_list)
		{
			String[] address = find_primary.split(":");
			
			if(hostAvailabilityCheck(address[0], address[1]))
			{
				if(isPrimaryServer(address[0], address[1]))
				{
					primary_location = address[0] + ":" + address[1];
					break;
				}
			}
		}
		
		//Finding the primary server ends
		
		if(!primary_location.isEmpty())
		{
			DirectoryServer_1.isPrimary = false;
			
			//Start recovering the files
			String[] address = primary_location.split(":");
			
			try
			{
				primary_directory_server = new Socket(address[0],Integer.valueOf(address[1]));
				
				primary_reader = new BufferedReader(new InputStreamReader(primary_directory_server.getInputStream()));
				
				primary_writer = new PrintWriter(primary_directory_server.getOutputStream(),true);
				
				String send_to_server = 1 + ":" + my_location;
				
				primary_writer.println(send_to_server);
				
				String[] list_of_files = primary_reader.readLine().split("~");
				
				String[] storage_node_location = primary_reader.readLine().split("~");
				
				File file_list = new File(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt");
				
				File storage_list = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
				
				if(file_list.exists())
				{
					//Delete file
					file_list.delete();
				}
				
				if(storage_list.exists())
				{
					//Delete file
					storage_list.delete();
				}
				
				//Write to list_of_files.txt
				File new_file_list = new File(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt");
				
				if(new_file_list.createNewFile())
				{
					PrintWriter fileWriter = new PrintWriter(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt");
					for(int i=0;i<list_of_files.length;i++)
					{
						fileWriter.append(list_of_files[i]);
						fileWriter.append("\n");
					}
					fileWriter.close();
				}
				else
				{
					System.out.println("File list_of_files.txt not created successfully");
				}
				
				File new_storage_list = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
				
				if(new_storage_list.createNewFile())
				{
					PrintWriter fileWriter = new PrintWriter(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
					for(int i=0;i<storage_node_location.length;i++)
					{
						if(!storage_node_location[i].isEmpty())
						{
							storage_node_list.add(storage_node_location[i]);
							
							fileWriter.append(storage_node_location[i]);
							fileWriter.append("\n");
						}
						

					}
					fileWriter.close();
				}
				else
				{
					System.out.println("File storage_node_location.txt not created successfully");
				}
				
				primary_directory_server.close();
				
				System.out.println("Recovering of files done successfully");
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			
			if(primary_directory_server != null && !primary_directory_server.isClosed())
			{
				try
				{
					primary_directory_server.close();
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
			}
			
			//End of recovering the files
			
		}
		else
		{
			DirectoryServer_1.isPrimary = true;
		}
		
	}

	//End of preprocess() function

	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//Preprocess started
		
		preprocess();
		
		System.out.println("Preprocess ended successfully");
		
		System.out.println("Primary : " + DirectoryServer_1.isPrimary);
		
		//Preprocess Ends
		
		
		
		
		//If I am secondary I need to make primary establish a socket connection with my server
		
		Socket primary_directory_server = null;
		
		if(!isPrimary)
		{
			//Find the primary
			primary_directory_server = null;
			
			BufferedReader primary_reader = null;
			
			PrintWriter primary_writer = null;
			
			String primary_location = "";
			
			//Finding the primary server starts
			
			for(String find_primary : other_directory_server_list)
			{
				String[] address = find_primary.split(":");
				
				if(hostAvailabilityCheck(address[0], address[1]))
				{
					if(isPrimaryServer(address[0], address[1]))
					{
						primary_location = address[0] + ":" + address[1];
						break;
					}
				}
			}
			
			//Finding the primary server ends
			if(!primary_location.isEmpty())
			{
				String[] address = primary_location.split(":");
				
				try
				{
					primary_directory_server = new Socket(address[0],Integer.valueOf(address[1]));
					
					primary_reader = new BufferedReader(new InputStreamReader(primary_directory_server.getInputStream()));
					
					primary_writer = new PrintWriter(primary_directory_server.getOutputStream(),true);
					
					String send_to_server = 5 + ":" + my_location;
					
					primary_writer.println(send_to_server);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				
				
			}
			else
			{
				System.out.println("There is no primary and I'm not the primary");
			}
			
			
		}
		
		//Primary Socket connection establishment ends
		
		
		//Start the server after recovering the files
		try {
			ServerSocket server_socket = new ServerSocket(4441);
			
			Socket client_socket = null;
			
			System.out.println("Server Started");
			
			while(true)
			{
				client_socket = server_socket.accept();
				/*
				if(primary_directory_server != null && !primary_directory_server.isClosed())
				{
					try
					{
						primary_directory_server.close();
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}
				*/
				new ClientHandler1(client_socket).start();
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}



class ClientHandler1 extends Thread
{
	Socket client_socket = null;
	
	BufferedReader client_reader = null;
	
	PrintWriter client_writer = null;
	
	//static String client_location = "";
	
	ThreadLocal<String>client_location = new ThreadLocal<String>(); 
	
	boolean that_thread = false;
	
	boolean storage_node_thread = false;
	
	public ClientHandler1(Socket soc) throws IOException
	{
		this.client_socket = soc;
		
		client_reader = new BufferedReader(new InputStreamReader(client_socket.getInputStream()));
		
		client_writer = new PrintWriter(client_socket.getOutputStream(),true);
	}
	
	//Start of each client thread
	
	public void run()
	{
		try
		{
			//Accept request from clients
			while(true)
			{
				String request_from_client = client_reader.readLine();
				
				String[] process_request = request_from_client.split(":");
				
				//client_location = process_request[1] + ":" + process_request[2];

				client_location.set(process_request[1] + ":" + process_request[2]);
				//Seperate the incoming request
				
				if(process_request[0].equals("1"))
				{
					//Preprocess request
					
					File list_of_files = new File(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt");
					
					String file_contents = "";
					
					BufferedReader fileReader = new BufferedReader(new FileReader(list_of_files));
					
					String line = "";
					
					while( (line = fileReader.readLine()) != null )
					{
						file_contents = file_contents.concat(line);
						file_contents = file_contents.concat("~");
					}
					
					//Return file contents to the client
					client_writer.println(file_contents);
					
					File storage_list = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
					
					file_contents = "";
					
					line = "";
					
					BufferedReader fileReaderStorage = new BufferedReader(new FileReader(storage_list));
					
					
					while( (line = fileReaderStorage.readLine()) != null )
					{
						file_contents = file_contents.concat(line);
						file_contents = file_contents.concat("~");
					}
					
					fileReaderStorage.close();
					
					//Return file contents to the client
					client_writer.println(file_contents);

					
					
				}
				else if(process_request[0].equals("2"))
				{
					//Check if the current directory server is primary or not
					
					String response = String.valueOf(DirectoryServer_1.isPrimary);
					
					client_writer.println(response);
				}
				else if(process_request[0].equals("3"))
				{
					//Return the list of available files in the storage nodes
					System.out.println("Storage node location " + client_location.get());
					
					File list_of_files = new File(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt");
					
					String file_contents = "";
					
					BufferedReader fileReader = new BufferedReader(new FileReader(list_of_files));
					
					String line = "";
					
					while( (line = fileReader.readLine()) != null )
					{
						file_contents = file_contents.concat(line);
						file_contents = file_contents.concat("~");
					}
					
					//Return file contents to the client
					client_writer.println(file_contents);
				}
				else if(process_request[0].equals("4"))
				{
					//Read from storage_node_location and send one location to the client
					
					
					
					File storage_list = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
					
					String file_contents = "";
					
					String line = "";
					
					BufferedReader fileReaderStorage = new BufferedReader(new FileReader(storage_list));
					
					
					while( (line = fileReaderStorage.readLine()) != null )
					{
						file_contents = file_contents.concat(line);
						file_contents = file_contents.concat("~");
					}
					
					if(file_contents.isEmpty())
					{
						client_writer.println("EMPTY");
					}
					
					fileReaderStorage.close();
					
					String[] storage_locations = file_contents.split("~");
					
					int storage_length = storage_locations.length;
					
					Random rand = new Random();
					
					String send_to_client = storage_locations[rand.nextInt(storage_length)];
					
					client_writer.println(send_to_client);
					
				}
				else if(process_request[0].equals("5"))
				{
					//Make a socket connection with the secondary server
					Thread.sleep(1000);
					
					String[] address = (process_request[1]+":"+process_request[2]).split(":");
					
					Socket connection_to_secondary = null;
					
					PrintWriter secondary_writer = null;
					
					try
					{
						connection_to_secondary = new Socket(address[0],Integer.valueOf(address[1]));
						
						secondary_writer = new PrintWriter(connection_to_secondary.getOutputStream(),true);
						
						String send_to_server = 6 + ":" + DirectoryServer_1.my_location;
						
						secondary_writer.println(send_to_server);
						
						
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}
				else if(process_request[0].equals("6"))
				{
					that_thread = true;
					System.out.println("Socket with primary established successfully");
				}
				else if(process_request[0].equals("7"))
				{
					//Host Availability Check
					//System.out.println("Host Availability Check");
				}
				else if(process_request[0].equals("8"))
				{
					//Add a storage node to the storage node file list
					System.out.println("8 was requested");
					
					DirectoryServer_1.storage_node_list.add(process_request[0]+":"+process_request[1]);
					
					FileWriter add_data = new FileWriter(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt",true);

					
					add_data.write(process_request[1] + ":" + process_request[2] + "\n");
					
					
					add_data.close();
					
					
					client_writer.println("Node Successfully added");
					
				}
				else if(process_request[0].equals("9"))
				{
					String storage_node_location = process_request[1] + ":" + process_request[2];
					
					String requested_client = process_request[3] + ":" + process_request[4];
					
					String file_name = process_request[5];
					
					File f = new File(DirectoryServer_1.directory_server_location+"\\"+"storage_node_location.txt");
					
					File temp_f = new File(DirectoryServer_1.directory_server_location+"\\"+"temp.txt");
					
					
					
					
					if(temp_f.createNewFile())
					{
						System.out.println("Temp file successfully created");
					}
					
					
					//Adding it to temp
					FileWriter add_data = new FileWriter(DirectoryServer_1.directory_server_location+"\\"+"temp.txt",true);
					
					add_data.write(storage_node_location + "\n");
					
					add_data.close();
					
					
					BufferedReader file_reader = new BufferedReader(new FileReader(f));
					
					//NEW CHANGE
					//file_reder close() is done twice
					String[] storage_node_locations = null;
					
					String temp_line = "";
					
					String temp_file_contents = "";
					
					while( (temp_line = file_reader.readLine()) != null)
					{
						temp_file_contents = temp_file_contents.concat(temp_line);
						
						temp_file_contents = temp_file_contents.concat("~");
					}
					
					file_reader.close();
					
					String[] storage_node_file_contents = temp_file_contents.split("~");
					
					String line = "";
					
					boolean isFailed = false;
					
					//THIS WHILE IS CHANGED TO A FOR
					for(int i=0;i<storage_node_file_contents.length;i++)
					{
						line = storage_node_file_contents[i];
						
						if(!line.equals(storage_node_location))
						{
							Socket temp_socket = null;
							
							PrintWriter temp_writer = null;
							
							BufferedReader temp_reader = null;
							
							String[] dest_storage_location = line.split(":");
							
							Thread.sleep(10000);
							
							while(true)
							{
								try
								{
									//You need to check if he's alive else change it to another node in temp
									//Do thread sleep here to check
									
									//Find a storage node location that is alive
									//For this read temp file
									boolean isOneExist = false;
									
									boolean curStorageExist = false;
									
									
									//Checking if current storage node exists and if atleast one exits starts
									File check_exists = new File(DirectoryServer_1.directory_server_location+"\\"+"temp.txt");
									
									if(!check_exists.exists())
									{
										isFailed = true;
										break;
									}
									
									BufferedReader temp_txt_reader = new BufferedReader(new FileReader(DirectoryServer_1.directory_server_location+"\\"+"temp.txt"));
									
									String line_j = "";
									
									while( (line_j = temp_txt_reader.readLine()) != null)
									{
										if(line_j != null && !line_j.isEmpty())
										{
											isOneExist = true;
											
											if(line_j.equals(storage_node_location))
											{
												curStorageExist = true;
											}
										}
									}
									
									temp_txt_reader.close();
									
									//Checking if current storage node exists and if atleast one exits starts ends
									
									
									if(!curStorageExist)
									{
										if(!isOneExist)
										{
											isFailed = true;
											break;
										}
										else
										{
											//Find a new storage node location that has the file
											BufferedReader temp_txt_reader_2 = new BufferedReader(new FileReader(DirectoryServer_1.directory_server_location+"\\"+"temp.txt"));
											
											String line_temp = "";
											
											while( (line_temp = temp_txt_reader_2.readLine()) != null)
											{
												if(line_temp != null && !line_temp.isEmpty())
												{
													storage_node_location = line_temp;
													break;
												}
											}
											
											temp_txt_reader_2.close();
											
										}
									}
									
									
									System.out.println("New storage node location " + storage_node_location);
									
									System.out.println("Node to be broadcasted " + dest_storage_location[0]+":"+dest_storage_location[1]);
									
									temp_socket = new Socket(dest_storage_location[0],Integer.valueOf(dest_storage_location[1]));
									
									temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
									
									temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
									
									//if(DirectoryServer_1.isPrimary)
									//{
										String send_to_server = 5 + ":" + storage_node_location + ":" + file_name;
									
										temp_writer.println(send_to_server);
									//}
									/*
									else
									{
										//Establish socket connection 
									}
									*/
									String status  = temp_reader.readLine();
									
									
									//CLose the socket here
									
									
									//THIS IS A NEW CHANGE JUST TO READ THE FILE CONTENTS
									//BUfferedReader temp_file_temporary = new BUfferedReader
									
									if(status.equals("SUCCESS"))
									{
										FileWriter add_storage_node = new FileWriter(DirectoryServer_1.directory_server_location+"\\"+"temp.txt",true);
										
										add_storage_node.write(dest_storage_location[0] + ":" + dest_storage_location[1] + "\n" );
										
										add_storage_node.close();
										
										System.out.println("File successfully downloaded by "+ dest_storage_location[0]+":"+dest_storage_location[1]);
										
										break;
									}
								}
								catch(Exception ex)
								{
									System.out.println("Destination storage node failed");
									break;
									
									//ex.printStackTrace();
								}
								
								//Compare the files
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

							}
							
							
							
							//Compare the files
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
							
							if(isFailed)
							{
								break;
							}
							
						}

					}
					
					file_reader.close();
					//End of while
					
					System.out.println("Did the upload fail " + isFailed);
					
					System.out.println("Primary : " +  DirectoryServer_1.isPrimary);
					/*
					if(isFailed && DirectoryServer_1.isPrimary)
					{
						client_writer.println("FAILURE");
					}
					*/
					

					
					
					//Delete the temp file
					
					File temp_file = new File(DirectoryServer_1.directory_server_location+"\\"+"temp.txt");
					
					if(temp_file.exists())
					{
						temp_file.delete();
						System.out.println("Temp deleted successfully");
					}
					
					
					//Notify the client if primary
					
					System.out.println("Before the client response " + DirectoryServer_1.isPrimary);
					
					if(DirectoryServer_1.isPrimary)
					{
						Socket temp_client_socket = null;
						
						PrintWriter temp_client_writer = null;
						
						try
						{
							String[] address_client = requested_client.split(":");
							

							temp_client_socket = new Socket(address_client[0],Integer.valueOf(address_client[1]));
							temp_client_writer = new PrintWriter(temp_client_socket.getOutputStream(),true);
							
							if(isFailed)
							{
								temp_client_writer.println("FAILURE");
							}
							else
							{
								
								//Update the list of files
								
								//THIS IS A NEW CHANGE
								FileWriter add_file = new FileWriter(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt",true);
								
								add_file.write(file_name + "\n");
								
								
								add_file.close();
								
								temp_client_writer.println("SUCCESS");
							}
							
							
							
							temp_client_socket.close();
							
						}
						catch(Exception ex)
						{
							ex.printStackTrace();
						}
						
						if(temp_client_socket != null && !temp_client_socket.isClosed())
						{
							try
							{
								temp_client_socket.close();
							}
							catch(Exception ex)
							{
								ex.printStackTrace();
							}
						}
						
						client_writer.println("SUCCESS");
			
					}
					else
					{
						//THIS IS A NEW CHANGE
						if(!isFailed)
						{
						FileWriter add_file = new FileWriter(DirectoryServer_1.directory_server_location+"\\"+"list_of_files.txt",true);
						
						add_file.write(file_name + "\n");
						
						add_file.close();
						}
					}
					
					
				}
				else if(process_request[0].equals("10"))
				{
					storage_node_thread = true;
				}
			}
		}
		catch(Exception ex)
		{
			
			if(that_thread)
			{
				System.out.println("SOCKET TO PRIMARY FAILED");
			}
			
			if(storage_node_thread)
			{
				System.out.println("STORAGE NODE OPEN SOCKET FAILED");
				if(client_location.get() == null)
				{
					System.out.println("STORAGE NODE LOCATION " + client_location.get());
				}
				else
				{
					System.out.println("STORAGE NODE LOCATION EMPTY" );
				}
				
				
				//Delete location from file
				try {
					DirectoryServer_1.delete_from_storage_list(client_location.get());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				System.out.println("Successfully removed");
				
			}
			
			
			if(client_location.get() == null)
			{
				System.out.println("EMPTY");
			}
			else
			{
				System.out.println("Client location " + client_location.get());
			}
			
			if(that_thread)
			{
				DirectoryServer_1.isPrimary = true;
				System.out.println("Directroy Server 1 " + DirectoryServer_1.isPrimary);
			}
			
			/*
			
			if(client_location.isEmpty())
			{
				
			}
			else if(client_location.equals("localhost:4442"))
			{
				String[] address = "localhost:4442".split(":");
				
				boolean isAlive = DirectoryServer_1.hostAvailabilityCheck(address[0], address[1]);
				
				System.out.println("Is 4442 alive " + isAlive);
				
				if(!isAlive && !DirectoryServer_1.isPrimary)
				{
					DirectoryServer_1.isPrimary = true;
				}
				
				System.out.println("Primary in catch :"+DirectoryServer_1.isPrimary);
			}
			*/
			//Close the socket if not closed
			if(client_socket != null && !client_socket.isClosed())
			{
				try
				{
					client_socket.close();
					//System.out.println("Socket closed successfully");
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				
				
			}
			
			
		}
	}
	
	//End of each client thread
}