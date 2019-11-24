package Storage_Node;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

import Directory_Server.DirectoryServer_1;

public class StorageNode {
	
	static String my_location;
	
	static String directory_location ; 
	
	static String[] directory_server_locations = {"localhost:4441","localhost:4442"};
	
	//static String[] list_of_files = null;
	
	static ArrayList<String> list_of_files = new ArrayList<String>();
	
	
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
		//System.out.println("Address "+ip_address+" "+port_numb);
		
		Socket temp_socket = null;
		
		BufferedReader primaryReader = null;
		
		PrintWriter primaryWriter = null;
		
		try
		{
			temp_socket = new Socket(ip_address,Integer.valueOf(port_numb));
			
			primaryReader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
			
			primaryWriter = new PrintWriter(temp_socket.getOutputStream(),true);
			
			//Return from directory server to check if it is primary or not
			
			String send_to_directory_server = 2 + ":" + my_location ;
			
			primaryWriter.println(send_to_directory_server);
			
			boolean primary = Boolean.valueOf(primaryReader.readLine()); 
			
			//System.out.println("I am primary "+ primary);
			
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
	
	
	
	public static void preprocess() throws IOException
	{
		//Contact the directory server and ask for the list of files to download
		
		//Delete all the files and create list_of_files alone
		File dir = new File(directory_location);
		
		for(File f: dir.listFiles())
		{
			if(!f.isDirectory())
			{
				f.delete();
			}
		}
		
		File new_file = new File(directory_location+"\\"+"list_of_files.txt");
		
		if(new_file.createNewFile())
		{
			System.out.println("File successfully created");
		}
		
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
		
		System.out.println("Primary Directory Server " + primary_directory_server_location);
		//Ask primary directory server for the list of files and
		Socket temp_socket = null;
		
		PrintWriter temp_writer = null;
		
		BufferedReader temp_reader = null;
		
		String[] file_list = null;
		try
		{
			String[] address = primary_directory_server_location.split(":");
			
			temp_socket = new Socket(address[0],Integer.valueOf(address[1]));
			
			temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
			
			temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
			
			String send_to_server = 3 + ":" + my_location;
			
			temp_writer.println(send_to_server);
			
			file_list = temp_reader.readLine().split("~");
			
			//Cloning the files
			
			PrintWriter fileWriter = new PrintWriter(directory_location+"\\"+"list_of_files.txt");
			for(int i=0;i<file_list.length;i++)
			{
				if(!file_list[i].isEmpty())
				{
					System.out.println(file_list[i]);
					fileWriter.append(file_list[i]);
					fileWriter.append("\n");
				}
			}
			fileWriter.close();
			
			for(int i=0;i<file_list.length;i++)
			{
				if(!file_list[i].isEmpty())
				{
					list_of_files.add(file_list[i]);
				}
				
			}
			
			temp_socket.close();
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		System.out.println("List of files successfully added");
		
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
		
		//Ask for files ends
		System.out.println("List of files " + file_list.length);
		for(int i=0;i<file_list.length;i++)
		{
			System.out.println(file_list[i]);
		}
		
		String storage_node_location = "" ;
		
		if(file_list.length == 0 || file_list[0].isEmpty())
		{
			//preprocess done
			//System.out.println("File list length " + file_list.length);
			//System.out.println("Preprocess successfully done");
			//return;
		}
		else
		{
			//Ask for a storage node location for download
			Socket temp_socket_download = null;
			
			PrintWriter temp_writer_download = null;
			
			BufferedReader temp_reader_download = null;
			
			try
			{
				String[] address = primary_directory_server_location.split(":");
				
				temp_socket_download = new Socket(address[0],Integer.valueOf(address[1]));
				
				temp_writer_download = new PrintWriter(temp_socket_download.getOutputStream(),true);
				
				temp_reader_download = new BufferedReader(new InputStreamReader(temp_socket_download.getInputStream()));
				
				String send_to_server = 4 + ":" + my_location;
				
				temp_writer_download.println(send_to_server);
				
				storage_node_location = temp_reader_download.readLine();
				
				System.out.println("Storage Node location " + storage_node_location);
				
				temp_socket_download.close();
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			if(temp_socket_download != null && !temp_socket_download.isClosed())
			{
				try
				{
					temp_socket_download.close();
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
			}
		}
		//Storage node location received 
		
		//Download Files from the storage Node
		String[] address = storage_node_location.split(":");
		
		System.out.println("Storage Node location " + storage_node_location);
		
		//System.out.println("file list size " + list_of_files.size());
		
		for(int i = 0 ; i < file_list.length;i++)
		{
			System.out.println(file_list[i]);
		}
		
		if(!storage_node_location.equals("EMPTY") && list_of_files != null)
		{
			
		for(int i=0;i<file_list.length;i++)
		{
			String file_name = file_list[i];
			
			Socket temp_socket_storage = null;
			
			PrintWriter temp_writer_storage = null;
			
			BufferedReader temp_reader_storage = null;
			
			try
			{
				
				System.out.println("Address  " + address[0] +":" + address[1]);
				
				if(!hostAvailabilityCheck(address[0],address[1]))
				{
					continue;
				}
				
				temp_socket_storage = new Socket(address[0],Integer.valueOf(address[1]));
		
				temp_writer_storage = new PrintWriter(temp_socket_storage.getOutputStream(),true);
				
				temp_reader_storage = new BufferedReader(new InputStreamReader(temp_socket_storage.getInputStream()));
				

				
				String send_to_server = 3 + ":" + my_location + ":" + file_name;
				
				System.out.println("Send to server " + send_to_server);
				
				temp_writer_storage.println(send_to_server);
				
				
				String[] file_contents = temp_reader_storage.readLine().split("~");
				
				//Create a file and add the file contents to the file
				
				File create_file = new File(directory_location+"\\"+file_name);
				
				if(create_file.createNewFile())
				{
					System.out.println("New file successfully created");
				}
				
				PrintWriter file_writer = new PrintWriter(directory_location+"\\"+file_name);
				
				for(int j=0;j<file_contents.length;j++)
				{
					file_writer.append(file_contents[j]);
					
					file_writer.append("\n");
				}
				
				file_writer.close();
				
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
			
		}
		}
		//Register with the directory server
		
		for(String server_location : directory_server_locations)
		{
			String[] split_address = server_location.split(":");
			
			Socket temp_socket_storage = null;
			
			PrintWriter temp_writer_storage = null;
			
			BufferedReader temp_reader_storage = null;
			
			try
			{
				if(!hostAvailabilityCheck(split_address[0], split_address[1]))
				{
					continue;
				}
				
				temp_socket_storage = new Socket(split_address[0],Integer.valueOf(split_address[1]));
				
				temp_writer_storage = new PrintWriter(temp_socket_storage.getOutputStream(),true);
				
				temp_reader_storage = new BufferedReader(new InputStreamReader(temp_socket_storage.getInputStream()));
				
				String send_to_server = 8 + ":" + my_location;
				
				temp_writer_storage.println(send_to_server);
				
				String response_from_server = temp_reader_storage.readLine();
				
				System.out.println("Response From server " + response_from_server);
				
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
			
		}
		
		
		//Register with Directory server ends
		System.out.println("Preprocess done successfully");
		
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Enter your IP Address and PORT(IP:PORT)");
		my_location = scan.next();
		
		System.out.println("Enter the files location");
		directory_location = scan.next();
		
		//Preprocess starts
			preprocess();
		//Preprocess Ends
			
			
			
		//Establish socket connection with both the directory servers
		Socket open_socket_directory_server1 = null;
		
		PrintWriter directory_server1_writer = null;
		try
		{
			open_socket_directory_server1 = new Socket("localhost",4441);
			
			String send_to_server = 10 + ":" + my_location;
			
			directory_server1_writer = new PrintWriter(open_socket_directory_server1.getOutputStream(),true);
			
			directory_server1_writer.println(send_to_server);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		
		Socket open_socket_directory_server2 = null;
		
		PrintWriter directory_server2_writer = null;
		try
		{
			open_socket_directory_server2 = new Socket("localhost",4442);
			
			String send_to_server = 10 + ":" + my_location;
			
			directory_server2_writer = new PrintWriter(open_socket_directory_server2.getOutputStream(),true);
			
			directory_server2_writer.println(send_to_server);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
			
		//Start the server
		String[] address = my_location.split(":");
		
		try
		{
			
			ServerSocket storage_node_server = new ServerSocket(Integer.valueOf(address[1]));
			
			Socket client_socket = null;
			
			while(true)
			{
				client_socket = storage_node_server.accept();
				
				new StorageNodeClient(client_socket).start();
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		

		

	}

}


class StorageNodeClient extends Thread
{
	Socket client_socket ;
	
	BufferedReader client_reader;
	
	PrintWriter client_writer;
	
	public StorageNodeClient(Socket soc)
	{
		
		try
		{
			this.client_socket = soc;
			
			client_reader = new BufferedReader(new InputStreamReader(client_socket.getInputStream()));
			
			client_writer = new PrintWriter(client_socket.getOutputStream(),true);			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

	}
	
	public void run()
	{
		try {
			
			String[] request_from_client = client_reader.readLine().split(":");
			
			if(request_from_client[0].equals("1"))
			{
				//Read the file
				
				System.out.println("Reqeust 1 received");
				
				
				String file_name = request_from_client[3];
				File read_file = new File(StorageNode.directory_location+"\\"+file_name);
				
				BufferedReader read_file_temp = new BufferedReader(new FileReader(read_file));
				
				String file_contents = "";
				
				String line = "";
				
				while( (line = read_file_temp.readLine()) != null)
				{
					file_contents = file_contents.concat(line);
					file_contents = file_contents.concat("~");
				}
				
				client_writer.println(file_contents);
			}
			else if(request_from_client[0].equals("2"))
			{
				//Add the file
				
				String client_location = request_from_client[1] + ":" + request_from_client[2] ;
				
				String file_name = request_from_client[3];
				
				String[] file_contents = client_reader.readLine().split("~");
				
				File f = new File(StorageNode.directory_location+"\\"+file_name);
				
				if(f.createNewFile())
				{
					System.out.println("File successfully created");
					
					BufferedWriter file_writer = new BufferedWriter(new FileWriter(StorageNode.directory_location+"\\"+file_name));
					
					for(int i=0;i<file_contents.length;i++)
					{
						file_writer.write(file_contents[i]);
						file_writer.write("\n");
					}
					
					file_writer.close();
				}
				else
				{
					System.out.println("File creation unsuccessful");
				}
				
				FileWriter add_file = new FileWriter(StorageNode.directory_location+"\\"+"list_of_files.txt",true);
				
				add_file.write(file_name + "\n");

				add_file.close();
				
				
				//Create two open sockets and send 9 request to both of them so that they can run concurrently
				
				Socket to_directoryserver_1 = null;
				
				PrintWriter server_writer1 = null;
				try
				{
					//YOU NEED TO CHECK ISHOST AVAILABILITY
					to_directoryserver_1 = new Socket("localhost",4441);
					
					server_writer1 = new PrintWriter(to_directoryserver_1.getOutputStream(),true);
					
					String send_to_server = 9 + ":" + StorageNode.my_location + ":"  + client_location + ":" + file_name;
					
					server_writer1.println(send_to_server);
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
				Socket to_directoryserver_2 = null;
				
				PrintWriter server_writer2 = null;
				try
				{
					to_directoryserver_2 = new Socket("localhost",4442);
					
					server_writer2 = new PrintWriter(to_directoryserver_2.getOutputStream(),true);
					
					String send_to_server = 9 + ":" + StorageNode.my_location + ":"  + client_location + ":" + file_name;
					
					server_writer2.println(send_to_server);
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
				/*
				for(String s: StorageNode.directory_server_locations)
				{
					String[] address = s.split(":");
					
					Socket temp_socket = null;
					
					BufferedReader temp_reader = null;
					
					PrintWriter temp_writer = null;
					
					try
					{
						//BIG CHANGE HERE !!!!!!!!!!!!!!!!!!!!
						if(!StorageNode.hostAvailabilityCheck(address[0], address[1]))
						{
							continue;
						}
						
						
						temp_socket = new Socket(address[0],Integer.valueOf(address[1]));
						
						temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
						
						temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
						
						String client_location = request_from_client[1] + ":" + request_from_client[2] ; 
						
						String send_to_server = 9 + ":" + StorageNode.my_location + ":"  + client_location + ":" + file_name;
						
						temp_writer.println(send_to_server);
						
						//After successfull broadcast and download close the socket????
						String status = temp_reader.readLine();
						
						
						
						if(status.equals("SUCCESS"))
						{
							temp_socket.close();
						}
						
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
					
					
					if(temp_socket != null && !temp_socket.isClosed())
					{
						try
						{
							temp_socket = null;
						}
						catch(Exception ex)
						{
							ex.printStackTrace();
						}
					}
				}
				*/
				
			}
			else if(request_from_client[0].equals("3"))
			{
				//Send the file contents
				System.out.println("REQUEST 3 RECEIVED");
				
				String file_name = request_from_client[3];
				
				File get_file = new File(StorageNode.directory_location+"\\"+file_name);
				
				BufferedReader file_reader = new BufferedReader(new FileReader(get_file));
				
				String line = "";
				
				String file_contents = "" ;
				
				while( (line = file_reader.readLine()) != null)
				{
					file_contents = file_contents.concat(line);
					
					
					file_contents = file_contents.concat("~");
				}
				
				file_reader.close();
				
				client_writer.println(file_contents);
			}
			else if(request_from_client[0].equals("4"))
			{
				//Get the file list
				
			}
			else if(request_from_client[0].equals("5"))
			{
				//Getting a message from directory server to download file from someone
				String ping_that_node = request_from_client[1] + ":" + request_from_client[2];
				
				String file_name = request_from_client[3];
				
				Socket temp_socket = null;
				
				BufferedReader temp_reader = null;
				
				PrintWriter temp_writer = null;
				
				try
				{
					
					//IT COULD FAIL HERE 
					//Thread.sleep(10000);
					
					String[] address = ping_that_node.split(":");
					
					temp_socket = new Socket(address[0],Integer.valueOf(address[1]));
					
					temp_reader = new BufferedReader(new InputStreamReader(temp_socket.getInputStream()));
					
					temp_writer = new PrintWriter(temp_socket.getOutputStream(),true);
					
					String send_to_server = 3 + ":" +StorageNode.my_location + ":" + file_name;
					
					temp_writer.println(send_to_server);
					
					String[] file_contents = temp_reader.readLine().split("~");
					
					File f = new File(StorageNode.directory_location+"\\"+file_name);
					
					System.out.println("File path " + StorageNode.directory_location + "\\" + file_name);
					
					boolean isExists = false;
					
					if(f.exists())
					{
						System.out.println("File already exists");
						client_writer.println("SUCCESS");
						isExists = true;
					}
					else
					{
						System.out.println("File doesnt exist");
					}
					
					if(f.createNewFile())
					{
						System.out.println("File successfully created");
						
						BufferedWriter file_writer = new BufferedWriter(new FileWriter(StorageNode.directory_location+"\\"+file_name));
						
						for(int i=0;i<file_contents.length;i++)
						{
							file_writer.write(file_contents[i]);
							file_writer.write("\n");
						}
						
						file_writer.close();
						
					
					}
					else
					{
						client_writer.println("SUCCESS");
						isExists = true;
						System.out.println("File creation unsuccessful");
					}
					
					//THIS ISEXIST FIX WORKED
					//FOR SOME REASON PRINTLN INSIDE IF ELSE NOT WORKING
					
					if(!isExists)
					{
						FileWriter add_file = new FileWriter(StorageNode.directory_location+"\\"+"list_of_files.txt",true);
						
						add_file.write(file_name + "\n");

						add_file.close();					
					}
					

					
					client_writer.println("SUCCESS");
					
					/*
					if(!StorageNode.isPrimary("localhost", "4441"))
					{
						client_writer.println("SUCCESS");
					}
					
					if(!StorageNode.isPrimary("localhost", "4442"))
					{
						client_writer.println("SUCCESS");
					}
					*/
					
				}
				catch(Exception ex)
				{
					client_writer.println("FAILURE");
					//ex.printStackTrace();
				}
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
				
	}
}