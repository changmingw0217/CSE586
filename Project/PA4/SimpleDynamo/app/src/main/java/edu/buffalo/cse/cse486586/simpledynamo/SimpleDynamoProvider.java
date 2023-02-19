package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.*;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final String REMOTE_PORT0 = "11108";
	private static final String REMOTE_PORT1 = "11112";
	private static final String REMOTE_PORT2 = "11116";
	private static final String REMOTE_PORT3 = "11120";
	private static final String REMOTE_PORT4 = "11124";
	private static final String[] REMOTE_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
	private static final int SERVER_PORT = 10000;
	private ArrayList<String> Nodes = new ArrayList<String>();
	private String pred1 = null;
	private String pred2 = null;
	private String succ1 = null;
	private String succ2 = null;
	private String self_port = null;
	private HashMap<String, String> token_port = new HashMap<String, String>();
	private Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
	private String node_id = null;
	private ArrayList<String> file_list = new ArrayList<String>();
	private ArrayList<String> pred1_files = new ArrayList<String>();
	private ArrayList<String> pred2_files = new ArrayList<String>();
	private ArrayList<String> self_files = new ArrayList<String>();
	private boolean sync = false;

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		self_port = portStr;

		try{
			node_id = genHash(self_port);
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		Log.i(TAG, "onCreate - portID: "+portStr+"; portNumber: "+myPort);
		Log.i(myPort, "Hash Key of this node: " + node_id);

		for (String s: REMOTE_PORTS){
			try{
				int port = Integer.parseInt(s)/2;
				String str_port = String.valueOf(port);
				String token = genHash(str_port);
				token_port.put(token,str_port);
				Nodes.add(token);
			}catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		Collections.sort(Nodes);

		int pos = Nodes.indexOf(node_id);

		if (pos == 0){
			pred1 = Nodes.get(4);
			pred2 = Nodes.get(3);
			succ1 = Nodes.get(1);
			succ2 = Nodes.get(2);
		}else if(pos == 1){
			pred1 = Nodes.get(0);
			pred2 = Nodes.get(4);
			succ1 = Nodes.get(2);
			succ2 = Nodes.get(3);
		}else if(pos == 2){
			pred1 = Nodes.get(1);
			pred2 = Nodes.get(0);
			succ1 = Nodes.get(3);
			succ2 = Nodes.get(4);
		}else if (pos == 3){
			pred1 = Nodes.get(2);
			pred2 = Nodes.get(1);
			succ1 = Nodes.get(4);
			succ2 = Nodes.get(0);
		}else {
			pred1 = Nodes.get(3);
			pred2 = Nodes.get(2);
			succ1 = Nodes.get(0);
			succ2 = Nodes.get(1);
		}

		Log.i("On Create", "Pred1 is " + token_port.get(pred1) + " Pred2 is " + token_port.get(pred2) + " succ1 is " + token_port.get(succ1) + " succ2 is " + token_port.get(succ2));

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		String[] file_list0 = getContext().fileList();


		for (String s: file_list0){
			getContext().deleteFile(s);
		}

		file_list.clear();
		pred1_files.clear();
		pred2_files.clear();

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null);
		sync = false;

		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			Socket sSocket;
			DataInputStream dis;
			String input;
			DataOutputStream dos;

			while (true) {
				try {
					sSocket = serverSocket.accept();


					dis = new DataInputStream(sSocket.getInputStream());
					dos = new DataOutputStream(sSocket.getOutputStream());
					input = dis.readUTF();


					String[] inputs = input.split("@");
					String command = inputs[0];
					Log.i("Sever Task", "The command is " + command);

					if (command.equals("Insert")){
						String key = inputs[1];
						String value = inputs[2];
						file_list.add(key);
						self_files.add(key);
						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.flush();
						outputStream.close();

						dos.writeUTF("ack");

						try {
							String succ1_port = token_port.get(succ1);
							Socket succ1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ1_port) * 2);
							String msg_to_send = "IReceive1" + "@" + key + "@" + value;
							DataOutputStream dos1 = new DataOutputStream(succ1_socket.getOutputStream());
							dos1.writeUTF(msg_to_send);
							dos1.flush();
							dos1.close();
							succ1_socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}

						try {
							String succ2_port = token_port.get(succ2);
							Socket succ2_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ2_port) * 2);
							String msg_to_send = "IReceive2" + "@" + key + "@" + value;
							DataOutputStream dos2 = new DataOutputStream(succ2_socket.getOutputStream());
							dos2.writeUTF(msg_to_send);
							dos2.flush();
							dos2.close();
							succ2_socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if (command.equals("IReceive1")){
						String key = inputs[1];
						String value = inputs[2];
						file_list.add(key);
						pred1_files.add(key);
						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.flush();
						outputStream.close();
					}else if (command.equals("IReceive2")){
						String key = inputs[1];
						String value = inputs[2];
						file_list.add(key);
						pred2_files.add(key);
						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.flush();
						outputStream.close();
					}else if (command.equals("Query")){
						if (inputs[1].equals("*")){
							String self_pair = "";
							if (file_list.size() > 0){
								StringBuilder stringBuilder1 = new StringBuilder();
								for (String s : file_list){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								self_pair = stringBuilder1.toString();
							}
							dos.writeUTF(self_pair);
//							if (succ1.equals(inputs[2])){
//								dos.writeUTF(self_pair);
//							}
//							else {
//								String succ_port = token_port.get(succ1);
//								Socket succ_s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//										Integer.parseInt(succ_port)*2);
//								DataOutputStream succ_dos = new DataOutputStream(succ_s.getOutputStream());
//								succ_dos.writeUTF(input);
//								DataInputStream succ_dis = new DataInputStream(succ_s.getInputStream());
//								String sucResult = succ_dis.readUTF();
//								String combineResult = self_pair + sucResult;
//								succ_dos.close();
//								succ_dis.close();
//								succ_s.close();
//
//								dos.writeUTF(combineResult);
//							}
						}else {
							String key = inputs[1];
							FileInputStream fis = getContext().openFileInput(key);
							InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
							StringBuilder stringBuilder = new StringBuilder();
							BufferedReader reader = new BufferedReader(inputStreamReader);

							String line = reader.readLine();
							stringBuilder.append(line);
							String content = stringBuilder.toString();
							String re = content;
							dos.writeUTF(re);
						}
					}else if (command.equals("Delete")){
						if (inputs[1].equals("*")){
							delete(uri, "@", null);
						}else {
							if (file_list.contains(inputs[1])){
								getContext().deleteFile(inputs[1]);
								file_list.remove(inputs[1]);
							}
							dos.writeUTF("ack");

							try {
								String succ1_port = token_port.get(succ1);
								Socket succ1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(succ1_port) * 2);
								String msg_to_send = "DReceive1@" + inputs[1];
								DataOutputStream dos1 = new DataOutputStream(succ1_socket.getOutputStream());
								dos1.writeUTF(msg_to_send);
								dos1.flush();
								dos1.close();
								succ1_socket.close();
							} catch (IOException e) {
								e.printStackTrace();
							}

							try {
								String succ2_port = token_port.get(succ2);
								Socket succ2_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(succ2_port) * 2);
								String msg_to_send = "DReceive2@" + inputs[1];
								DataOutputStream dos2 = new DataOutputStream(succ2_socket.getOutputStream());
								dos2.writeUTF(msg_to_send);
								dos2.flush();
								dos2.close();
								succ2_socket.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}else if (command.equals("DReceive1")){
						if (file_list.contains(inputs[1])){
							getContext().deleteFile(inputs[1]);
							file_list.remove(inputs[1]);
							pred1_files.remove(inputs[1]);
						}
					}else if (command.equals("DReceive2")){
						if (file_list.contains(inputs[1])){
							getContext().deleteFile(inputs[1]);
							file_list.remove(inputs[1]);
							pred2_files.remove(inputs[1]);
						}
					}else if (command.equals("Sync")){
//						if (inputs[1].equals("Succ1") || inputs[1].equals("Succ2")){
//							if (self_files.size() == 0){
//								dos.writeUTF("No");
//								dos.flush();
//							}else {
//
//								StringBuilder stringBuilder1 = new StringBuilder();
//								for (String s : self_files){
//									FileInputStream fis = getContext().openFileInput(s);
//									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
//									StringBuilder stringBuilder = new StringBuilder();
//									BufferedReader reader = new BufferedReader(inputStreamReader);
//									String line = reader.readLine();
//									stringBuilder.append(line);
//									String content = stringBuilder.toString();
//
//									String pair = s + "-" + content + "@";
//									stringBuilder1.append(pair);
//								}
//								String pairs = stringBuilder1.toString();
//								dos.writeUTF(pairs);
//								dos.flush();
//							}
						if (inputs[1].equals("Succ1")){
							if (self_files.size() == 0 && pred1_files.size() == 0){
								dos.writeUTF("No");
								dos.flush();
							}else if (pred1_files.size() != 0 && self_files.size() == 0){
								StringBuilder stringBuilder1 = new StringBuilder();
								for (String s : pred1_files){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								String pairs = stringBuilder1.toString();
								String msg = "Pred2%" + pairs;
								dos.writeUTF(msg);
								dos.flush();
							}else if (self_files.size() != 0 && pred1_files.size() == 0){
								StringBuilder stringBuilder1 = new StringBuilder();
								for (String s : self_files){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								String pairs = stringBuilder1.toString();
								String msg = "Pred1%" + pairs;
								dos.writeUTF(msg);
								dos.flush();
							}else{

								StringBuilder stringBuilder1 = new StringBuilder();
								for (String s : self_files){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								stringBuilder1.append("%");
								for (String s : pred1_files){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								String pairs = stringBuilder1.toString();
								String msg = "Both%" + pairs;
								dos.writeUTF(msg);
								dos.flush();
							}
						}else {
							if (pred1_files.size() == 0){
								dos.writeUTF("No");
								dos.flush();
							}else {
								StringBuilder stringBuilder1 = new StringBuilder();
								for (String s : pred1_files){
									FileInputStream fis = getContext().openFileInput(s);
									InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
									StringBuilder stringBuilder = new StringBuilder();
									BufferedReader reader = new BufferedReader(inputStreamReader);
									String line = reader.readLine();
									stringBuilder.append(line);
									String content = stringBuilder.toString();

									String pair = s + "-" + content + "@";
									stringBuilder1.append(pair);
								}
								String pairs = stringBuilder1.toString();
								dos.writeUTF(pairs);
								dos.flush();
							}
						}
					}else if (command.equals("InsertR")){
						String key = inputs[1];
						String value = inputs[2];
						file_list.add(key);
						pred1_files.add(key);
						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.flush();
						outputStream.close();

						try {
							String succ1_port = token_port.get(succ1);
							Socket succ1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ1_port) * 2);
							String msg_to_send = "IReceive2" + "@" + key + "@" + value;
							DataOutputStream dos1 = new DataOutputStream(succ1_socket.getOutputStream());
							dos1.writeUTF(msg_to_send);
							dos1.flush();
							dos1.close();
							succ1_socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if (command.equals("DeleteR")){
						if (file_list.contains(inputs[1])){
							getContext().deleteFile(inputs[1]);
							file_list.remove(inputs[1]);
							pred1_files.remove(inputs[1]);
						}

						try {
							String succ1_port = token_port.get(succ1);
							Socket succ1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ1_port) * 2);
							String msg_to_send = "DReceive2@" + inputs[1];
							DataOutputStream dos1 = new DataOutputStream(succ1_socket.getOutputStream());
							dos1.writeUTF(msg_to_send);
							dos1.flush();
							dos1.close();
							succ1_socket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {

			sync = true;
			Date d = new Date();
			long time1 = d.getTime();
			Log.i("Client Task", "The sync state is " + sync + " And time is" + time1);
			try{
				String pred1_port = token_port.get(pred1);
				Socket pred1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(pred1_port) * 2);

				String msg = "Sync@Succ1";

				DataOutputStream pred1_dos = new DataOutputStream(pred1_socket.getOutputStream());
				pred1_dos.writeUTF(msg);
				pred1_dos.flush();

				DataInputStream pred1_dis = new DataInputStream(pred1_socket.getInputStream());
				String pred1_in = pred1_dis.readUTF();

				Log.i("Client Task", " The mag from pred1 is " + pred1_in);

				if (!pred1_in.equals("No")){

					Log.i("ClientTask", "restore pred1 files");
					String mode = pred1_in.split("%")[0];
					if (mode.equals("Pred2")){
						String pair = pred1_in.split("%")[1];
						String[] each_pair = pair.split("@");

						for (String str: each_pair){
							String key = str.split("-")[0];
							String content = str.split("-")[1];
							file_list.add(key);
							pred2_files.add(key);
							FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(content.getBytes());
							outputStream.flush();
							outputStream.close();
						}

					}else if (mode.equals("Pred1")){
						String pair = pred1_in.split("%")[1];
						String[] each_pair = pair.split("@");

						for (String str: each_pair){
							String key = str.split("-")[0];
							String content = str.split("-")[1];
							file_list.add(key);
							pred1_files.add(key);
							FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(content.getBytes());
							outputStream.flush();
							outputStream.close();
						}
					}else if (mode.equals("Both")){
						String pred1_pair = pred1_in.split("%")[1];
						String pred2_pair = pred1_in.split("%")[2];

						String[] pred1_pairs = pred1_pair.split("@");
						String[] pred2_pairs = pred2_pair.split("@");

						for (String str: pred1_pairs){
							String key = str.split("-")[0];
							String content = str.split("-")[1];
							file_list.add(key);
							pred1_files.add(key);
							FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(content.getBytes());
							outputStream.flush();
							outputStream.close();
						}
						for (String str: pred2_pairs){
							String key = str.split("-")[0];
							String content = str.split("-")[1];
							file_list.add(key);
							pred2_files.add(key);
							FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(content.getBytes());
							outputStream.flush();
							outputStream.close();
						}
					}
				}
				Log.i("Client Task", " After restore the pred1 file size is " + pred1_files.size());
				pred1_dos.close();
				pred1_dis.close();
				pred1_socket.close();

			}catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}

//			try{
//				String pred2_port = token_port.get(pred2);
//				Socket pred2_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(pred2_port) * 2);
//
//				String msg = "Sync@Succ2";
//
//				DataOutputStream pred2_dos = new DataOutputStream(pred2_socket.getOutputStream());
//				pred2_dos.writeUTF(msg);
//				pred2_dos.flush();
//
//				DataInputStream pred2_dis = new DataInputStream(pred2_socket.getInputStream());
//				String pred2_in = pred2_dis.readUTF();
//
//				Log.i("Client Task", " The mag from pred2 is " + pred2_in);
//
//				if (!pred2_in.equals("No")){
//
//					Log.i("ClientTask", "restore pred2 files");
//					String[] each_pair = pred2_in.split("@");
//
//					for (String str: each_pair){
//						String key = str.split("-")[0];
//						String content = str.split("-")[1];
//						file_list.add(key);
//						pred2_files.add(key);
//						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
//						outputStream.write(content.getBytes());
//						outputStream.flush();
//						outputStream.close();
//					}
//				}
//
//				Log.i("Client Task", " After restore the pred2 file size is " + pred2_files.size());
//
//				pred2_dos.close();
//				pred2_dis.close();
//				pred2_socket.close();
//
//			}catch (UnknownHostException e) {
//				Log.e(TAG, "ClientTask UnknownHostException");
//			} catch (IOException e) {
//				Log.e(TAG, "ClientTask socket IOException");
//			}

			try{
				String succ1_port = token_port.get(succ1);
				Socket succ1_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(succ1_port) * 2);
				String msg_to_send = "Sync@Pred1";
				DataOutputStream succ1_dos = new DataOutputStream(succ1_socket.getOutputStream());
				succ1_dos.writeUTF(msg_to_send);
				succ1_dos.flush();

				DataInputStream succ1_dis = new DataInputStream(succ1_socket.getInputStream());
				String succ1_in = succ1_dis.readUTF();
				Log.i("Client Task", " The mag from succ1 is " + succ1_in);

				if (!succ1_in.equals("No")){
					Log.i("ClientTask", "restore succ files");
					String[] each_pair = succ1_in.split("@");

					for (String str: each_pair){
						String key = str.split("-")[0];
						String content = str.split("-")[1];
						file_list.add(key);
						self_files.add(key);
						FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(content.getBytes());
						outputStream.flush();
						outputStream.close();
					}
				}
				Log.i("Client Task", " After restore the self file size is " + self_files.size());


				succ1_dos.close();
				succ1_dis.close();
				succ1_socket.close();

			}catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}

			Log.i("Client Task", " After restore the total file size is " + file_list.size());
			sync = false;
			long time2 = d.getTime();
			Log.i("Client Task", "The sync state is " + sync + " And time is" + time2);

			return null;
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		try{

			if (selection.equals("@")){
				for (String s: file_list){
					getContext().deleteFile(s);
					file_list.remove(s);
				}
			}else if (selection.equals("*")){
				for (String s: file_list){
					getContext().deleteFile(s);
					file_list.remove(s);
				}

				for (String node: Nodes){
					String po = token_port.get(node);
					if (!po.equals(self_port)){
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(po)*2);

						String msg_to_send = "Delete@*";
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						dos.writeUTF(msg_to_send);

						dos.close();
						socket.close();
					}
				}
			}else {
				String g_node = goal_node(selection);
				try{
					String port = token_port.get(g_node);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port)*2);
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					String msg_to_send = "Delete@" + selection;
					dos.writeUTF(msg_to_send);

					DataInputStream dis = new DataInputStream(socket.getInputStream());
					String ack = dis.readUTF();

					dos.close();
					socket.close();
				}catch (IOException e){

					int g_index = Nodes.indexOf(g_node);
					int r_index;
					if (g_index == Nodes.size() - 1){
						r_index = 0;
					}else {
						r_index = g_index + 1;
					}
					String r_node = Nodes.get(r_index);
					try{
						String port = token_port.get(r_node);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port)*2);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						String msg_to_send = "DeleteR@" + selection;
						dos.writeUTF(msg_to_send);
						dos.close();
						socket.close();
					}catch (IOException e1){
						e1.printStackTrace();
					}
				}
			}

		}catch (Exception e){
			Log.i("Delete", "Delete failed + " + e);
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String g_node = goal_node(key);
		try{
			while (sync){
				try{
					Thread.sleep(200);
					Log.e ("Insert","Wating sync");
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			String port = token_port.get(g_node);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port)*2);
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

			String msg_to_send = "Insert@" + key + "@" + value ;
			dos.writeUTF(msg_to_send);

			DataInputStream dis = new DataInputStream(socket.getInputStream());
			String ack = dis.readUTF();

			dos.close();
			socket.close();
		} catch (IOException e){

			Log.i("Insert", "No response");


			int g_index = Nodes.indexOf(g_node);

			int r_index;
			if (g_index == Nodes.size() - 1){
				r_index = 0;
			}else {
				r_index = g_index + 1;
			}

			String r_node = Nodes.get(r_index);
			Log.i("Insert", "The re node is" + token_port.get(r_node));
			try{
				String port = token_port.get(r_node);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port)*2);
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				String msg_to_send = "InsertR@" + key + "@" + value ;
				dos.writeUTF(msg_to_send);
				dos.close();
				socket.close();
			}catch (IOException e1){
				e1.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String[] columnNames = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columnNames);

		try{

			if (selection.equals("@")){
				String[] file_list = getContext().fileList();
				for (String s : file_list) {
					FileInputStream fis = getContext().openFileInput(s);
					InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
					StringBuilder stringBuilder = new StringBuilder();
					BufferedReader reader = new BufferedReader(inputStreamReader);

					String line = reader.readLine();
					stringBuilder.append(line);
					String content = stringBuilder.toString();
					String[] result = {s, content};
					cursor.addRow(result);
					fis.close();
					inputStreamReader.close();
					reader.close();
				}
			}else if (selection.equals("*")){
				String self_pair = "";

				if (file_list.size() > 0){
					StringBuilder stringBuilder1 = new StringBuilder();
					for (String s : file_list){
						FileInputStream fis = getContext().openFileInput(s);
						InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
						StringBuilder stringBuilder = new StringBuilder();
						BufferedReader reader = new BufferedReader(inputStreamReader);
						String line = reader.readLine();
						stringBuilder.append(line);
						String content = stringBuilder.toString();

						String pair = s + "-" + content + "@";
						stringBuilder1.append(pair);
					}
					self_pair = stringBuilder1.toString();
				}
				String pairs = "";
				StringBuilder Sb = new StringBuilder();

				for (String s: Nodes){
					if (!s.equals(node_id)){
						String port = token_port.get(s);
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port)*2);
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							String cv = "Query@*";
							dos.writeUTF(cv);
							DataInputStream dis = new DataInputStream(socket.getInputStream());
							String result = dis.readUTF();
							Sb.append(result);
						}catch (IOException e){
							e.printStackTrace();
						}
					}
				}

				pairs = Sb.toString();
				String all_pairs = self_pair + pairs;

				String[] each_pair = all_pairs.split("@");

				if (each_pair.length > 0){
					for (String str: each_pair){
						String key = str.split("-")[0];
						String content = str.split("-")[1];
						String[] key_content = {key, content};

						cursor.addRow(key_content);

					}
				}

//				try{
//					String succ_port = token_port.get(succ1);
//					Socket succ_s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//							Integer.parseInt(succ_port)*2);
//					DataOutputStream succ_out = new DataOutputStream(succ_s.getOutputStream());
//					String msg_to_send = "Query@*@" + node_id;
//					succ_out.writeUTF(msg_to_send);
//
//					DataInputStream succ_in = new DataInputStream(succ_s.getInputStream());
//					String pairs = succ_in.readUTF();
//
//					String self_pair = "";
//					if (file_list.size() > 0){
//						StringBuilder stringBuilder1 = new StringBuilder();
//						for (String s : file_list){
//							FileInputStream fis = getContext().openFileInput(s);
//							InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
//							StringBuilder stringBuilder = new StringBuilder();
//							BufferedReader reader = new BufferedReader(inputStreamReader);
//							String line = reader.readLine();
//							stringBuilder.append(line);
//							String content = stringBuilder.toString();
//
//							String pair = s + "-" + content + "@";
//							stringBuilder1.append(pair);
//						}
//						self_pair = stringBuilder1.toString();
//					}
//
//					String all_pairs = self_pair + pairs;
//
//					String[] each_pair = all_pairs.split("@");
//
//					ArrayList<String> added_pair = new ArrayList<String>();
//
//					if (each_pair.length > 0){
//						for (String str: each_pair){
//							String key = str.split("-")[0];
//							String content = str.split("-")[1];
//							String[] key_content = {key, content};
//							if (!added_pair.contains(key)){
//								cursor.addRow(key_content);
//								added_pair.add(key);
//							}
//						}
//					}
//
//				}catch (IOException e){
//					e.printStackTrace();
//				}
			}else{
				while (sync){
					try{
						Thread.sleep(200);
						Log.e ("Query","Wating sync");
					} catch (Exception e)
					{
						e.printStackTrace();
					}
				}

				String g_node = goal_node(selection);
				try{
					String port = token_port.get(g_node);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port)*2);
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					String cv = "Query@" + selection;
					dos.writeUTF(cv);
					DataInputStream dis = new DataInputStream(socket.getInputStream());
					String result = dis.readUTF();
					String[] add = {selection, result};
					cursor.addRow(add);

					dos.close();
					dis.close();
					socket.close();
				}catch (IOException e){
					int g_index = Nodes.indexOf(g_node);
					int r_index;
					if (g_index == Nodes.size() - 1){
						r_index = 0;
					}else {
						r_index = g_index + 1;
					}
					String r_node = Nodes.get(r_index);
					try{
						String port = token_port.get(r_node);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port)*2);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						String msg_to_send = "Query@" + selection;
						dos.writeUTF(msg_to_send);

						DataInputStream dis = new DataInputStream(socket.getInputStream());
						String result = dis.readUTF();
						String[] add = {selection, result};
						cursor.addRow(add);


						dos.close();
						dis.close();
						socket.close();
					}catch (IOException e1){
						e1.printStackTrace();
					}

				}
			}
		}catch (Exception e) {
			Log.e("Query Failed", e.toString());
		}

		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public String goal_node(String input){
		try{

			for (int i = 0; i < Nodes.size();i++){
				if (i == 0){
					String hashed_key = genHash(input);
					String hashed_curr = Nodes.get(0);
					String hashed_pred = Nodes.get(4);

					boolean case1 = (hashed_curr.compareTo(hashed_pred) > 0 && hashed_key.compareTo(hashed_curr) <= 0 && hashed_key.compareTo(hashed_pred) > 0);
					boolean case2 = (hashed_pred.compareTo(hashed_curr) > 0 && (hashed_key.compareTo(hashed_pred) > 0 || hashed_key.compareTo(hashed_curr) <= 0));

					if( case1 || case2){
						return Nodes.get(0);
					}
				}else {
					String hashed_key = genHash(input);
					String hashed_curr = Nodes.get(i);
					String hashed_pred = Nodes.get(i-1);

					boolean case1 = (hashed_curr.compareTo(hashed_pred) > 0 && hashed_key.compareTo(hashed_curr) <= 0 && hashed_key.compareTo(hashed_pred) > 0);
					boolean case2 = (hashed_pred.compareTo(hashed_curr) > 0 && (hashed_key.compareTo(hashed_pred) > 0 || hashed_key.compareTo(hashed_curr) <= 0));

					if( case1 || case2){
						return Nodes.get(i);
					}
				}
			}
		}catch (NoSuchAlgorithmException e){
			Log.e("findDestination", "can't hash");
		}
		return null;
	}
}
