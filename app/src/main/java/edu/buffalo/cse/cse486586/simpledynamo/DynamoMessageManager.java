package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
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
                                out.println(MyUtils.hashMapToString(result));
                            }
                            case QUERY_ALL_GLOBAL: {
                                HashMap<String, String> result = dynamo.getAllGlobal();
                                out.println(MyUtils.hashMapToString(result));
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
                                out.println(MyUtils.readFile(dynamo.readReplica(key)));
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
}