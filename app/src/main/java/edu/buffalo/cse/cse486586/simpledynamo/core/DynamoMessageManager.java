package edu.buffalo.cse.cse486586.simpledynamo.core;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * An asynchronous task that opens a socket in the background and waits for
 * incoming requests and calls the respective methods of the dynamo as per
 * the type of message received.
 *
 * Created by utsav on 1/27/16.
 */
class DynamoMessageManager extends AsyncTask<ServerSocket, String, Void> {
    private final String TAG = "DynamoMessageManager";
    private Dynamo dynamo;

    public DynamoMessageManager(Dynamo dynamo) {
        this.dynamo = dynamo;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        Socket clientSocket;
        try {
            while (true) {
                clientSocket = serverSocket.accept();
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                Object object;
                try {
                    object = ois.readObject();
                    if (object instanceof DHTMessage) {
                        DHTMessage dhtMessage = (DHTMessage) object;
                        switch (dhtMessage.getMsgType()) {
                            case INSERT: {
                                String keyValue[] = dhtMessage.getMsg().split(",");
                                dynamo.put(keyValue[0], keyValue[1]);
                                break;
                            }
                            case QUERY:
                                out.println(dynamo.getValue(dhtMessage.getMsg()));
                                break;
                            case QUERY_ALL_LOCAL: {
                                HashMap<String, String> result = dynamo.getAllLocal();
                                out.println(hashMapToString(result));
                            }
                            case QUERY_ALL_GLOBAL: {
                                HashMap<String, String> result = dynamo.getAllGlobal();
                                out.println(hashMapToString(result));
                            }
                            case DELETE:
                                dynamo.remove(dhtMessage.getMsg());
                                break;
                            case DELETE_ALL_LOCAL:
                                dynamo.removeAllLocal();
                                break;
                            case DELETE_ALL_GLOBAL:
                                dynamo.removeAllGlobal();
                                break;
                            case REPLICATE: {
                                String keyValue[] = dhtMessage.getMsg().split(",");
                                dynamo.writeReplica(keyValue[0], keyValue[1]);
                                break;
                            }
                            case FETCH_REPLICA: {
                                String key = dhtMessage.getMsg();
                                out.println(dynamo.readReplica(key));
                                break;
                            }
                            case DELETE_REPLICA: {
                                String key = dhtMessage.getMsg();
                                dynamo.deleteReplica(key);
                                break;
                            }
                            default:
                                Log.e(TAG, "rcvd illegal msg type");
                                break;
                        }
                    } else {
                        Log.d(TAG, "received illegal object");
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String hashMapToString(HashMap<String, String> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            e.getKey();
            sb.append(",");
            e.getValue();
            sb.append("|");
        }
        return sb.toString();
    }
}