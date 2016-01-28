package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by utsav on 1/27/16.
 */
public class Dynamo {

    private final int SERVER_PORT;
    private final String TAG = "Dynamo";
    private final String myPort;
    HashMap<String, NodePair> joinedNodes = new HashMap<>();
    Context ctx;

    public Dynamo(int port, Context ctx) {
        this.SERVER_PORT = port;
        this.ctx = ctx;
        TelephonyManager tel = (TelephonyManager) ctx.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new DynamoMessageManager(this).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getLocalizedMessage());
        }
        new AsyncTask<Void, Void, Void>() {
            @Override
            protected Void doInBackground(Void... params) {
                recoverFailedMessages();
                return null;
            }
        }.execute();
    }

    public void addNode(String node, String prev, String next) {
        joinedNodes.put(node, new NodePair(prev, next));
    }

    public void put(String key, String value) {
        String values = key + "," + value;
        if (isCorrectNode(key)) {
            writeToFile(key, value);
            sendReplicateMsg(values, myPort);
        } else {
            String correctNode = findCorrectNode(key);
            DHTMessage insertDhtMessage = new DHTMessage(values, DHTMessage.MsgType.REPLICATE, myPort);
            sendMessage(insertDhtMessage, correctNode);
            sendReplicateMsg(values, correctNode);
        }
    }

    public HashMap<String, String> getAllLocal() {
        HashMap<String, String> result = new HashMap<>();
        for (File file : ctx.getFilesDir().listFiles()) {
            try {
                result.put(file.getName(), MyUtils.readFile(ctx.openFileInput(file.getName())));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public HashMap<String, String> getAllGlobal() {
        HashMap<String, String> result = new HashMap<>();

        for (File file : ctx.getFilesDir().listFiles()) {
            try {
                result.put(file.getName(), MyUtils.readFile(ctx.openFileInput(file.getName())));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        result.putAll(queryAllBroadcast());
        return result;
    }

    public String getValue(String key) {
        if (isCorrectNode(key)) {
            try {
                return MyUtils.readFile(ctx.openFileInput(key));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            Log.d(TAG, "sending to successor");
            String correctNode = findCorrectNode(key);
            DHTMessage queryMsg = new DHTMessage(key, DHTMessage.MsgType.QUERY, myPort);
            try {
                return sendMessageSync(queryMsg, correctNode);
            } catch (IOException e) {
                loge("failed to send query to correct node, trying to fetch from replica");
                e.printStackTrace();
                return fetchFromReplica(key, correctNode);
            }
        }
        return null;
    }

    public void remove(String key) {
        if (isCorrectNode(key)) {
            Log.d(TAG, "correct node");
            ctx.deleteFile(key);
            sendDeleteReplicaMsg(key);
        } else {
            String correctNode = findCorrectNode(key);
            Log.d(TAG, "sending to correct Node");
            DHTMessage queryMsg = new DHTMessage(key, DHTMessage.MsgType.DELETE, myPort);
            sendMessage(queryMsg, correctNode);
        }
    }

    public void removeAllGlobal() {
        removeAllLocal();
        for (String node : joinedNodes.keySet()) {
            if (!node.equals(myPort)) {
                DHTMessage msg = new DHTMessage("", DHTMessage.MsgType.DELETE_ALL_LOCAL, myPort);
                sendMessage(msg, node);
            }
        }
    }

    public void removeAllLocal() {
        for (File file : ctx.getFilesDir().listFiles()) {
            ctx.deleteFile(file.getName());
            sendDeleteReplicaMsg(file.getName());
        }
    }

    public void writeReplica(String key, String value) {
        FileOutputStream outputStream;
        try {
            outputStream = ctx.openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
    }

    public FileInputStream readReplica(String key) {
        try {
            return ctx.openFileInput(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void deleteReplica(String key) {
        ctx.deleteFile(key);
    }

    private void writeToFile(String filename, String fileContent) {
        FileOutputStream outputStream;
        try {
            outputStream = ctx.openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(fileContent.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String fetchFromReplica(String key, String correctNode) {
        String[] replica = getReplicas(correctNode);
        if (replica[0].equals(myPort) || replica[1].equals(myPort)) {
            try {
                return MyUtils.readFile(ctx.openFileInput(key));
            } catch (IOException e1) {
                loge("failed to fetch from local replica");
                e1.printStackTrace();
            }
        } else {
            loge(" trying to fetch from replica[0] " + replica[0]);
            DHTMessage dhtMessage = new DHTMessage(key, DHTMessage.MsgType.FETCH_REPLICA, myPort);
            try {
                return sendMessageSync(dhtMessage, replica[0]);
            } catch (IOException e1) {
                e1.printStackTrace();
                loge("failed to fetch from replica 0 trying from replica 1 " + replica[1]);
                DHTMessage dhtMessage2 = new DHTMessage(key, DHTMessage.MsgType.FETCH_REPLICA, myPort);
                try {
                    return sendMessageSync(dhtMessage2, replica[1]);
                } catch (IOException e2) {
                    e2.printStackTrace();
                    loge("Unable to fetch from replicas");
                }
            }
        }
        return null;
    }

    private String[] getNodesReplicatedFrom(String node) {
        String[] result = new String[2];
        result[0] = joinedNodes.get(node).predecessor;
        result[1] = joinedNodes.get(result[0]).predecessor;
        return result;
    }

    private void recoverFailedMessages() {
        HashMap<String, String> result = new HashMap<>();
        DHTMessage queryMsg = new DHTMessage("", DHTMessage.MsgType.QUERY_ALL_LOCAL, myPort);
        for (String replica : getReplicas(myPort)) {
            try {
                result.putAll(MyUtils.convertToHashMap(sendMessageSync(queryMsg, replica)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (String replicatedFrom : getNodesReplicatedFrom(myPort)) {
            try {
                result.putAll(MyUtils.convertToHashMap(sendMessageSync(queryMsg, replicatedFrom)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (Map.Entry<String, String> e : result.entrySet()) {
            String pre1 = joinedNodes.get(myPort).predecessor;
            String pre2 = joinedNodes.get(pre1).predecessor;
            String key = e.getKey();
            String value = e.getValue();
            if (isCorrectNode(key) || isCorrectNode(key, pre1) || isCorrectNode(key, pre2)) {
                try {
                    MyUtils.writeToFile(ctx.openFileOutput(key, Context.MODE_PRIVATE), value);
                } catch (FileNotFoundException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void sendDeleteReplicaMsg(String fileName) {
        DHTMessage dhtmsg = new DHTMessage(fileName, DHTMessage.MsgType.DELETE_REPLICA, myPort);
        for (String replica : getReplicas(myPort)) {
            sendMessage(dhtmsg, replica);
        }
    }

    private HashMap<String, String> queryAllBroadcast() {
        HashMap<String, String> result = new HashMap<>();
        if (joinedNodes.size() > 1) {
            for (String node : joinedNodes.keySet()) {
                if (node.equals(myPort)) continue;
                DHTMessage msg = new DHTMessage("", DHTMessage.MsgType.QUERY_ALL_LOCAL, myPort);
                try {
                    result.putAll(MyUtils.convertToHashMap(sendMessageSync(msg, node)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    private boolean isCorrectNode(String key) {
        return joinedNodes.size() == 1 || isCorrectNode(key, myPort);
    }

    private boolean isCorrectNode(String key, String node) {
        String pred_port = joinedNodes.get(node).predecessor;
        try {
            String id = MyUtils.genHash(key),
                    pred_id = MyUtils.genHash(MyUtils.getNodeIdFromPort(pred_port)),
                    my_id = MyUtils.genHash(MyUtils.getNodeIdFromPort(node));
            if ((id.compareTo(pred_id) > 0 && id.compareTo(my_id) <= 0) ||
                    ((my_id.compareTo(pred_id) < 0) && id.compareTo(pred_id) > 0)
                    || ((my_id.compareTo(pred_id) < 0) && id.compareTo(my_id) < 0)) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void sendReplicateMsg(String msg, String node) {
        for (String replica : getReplicas(node)) {
            sendMessage(new DHTMessage(msg, DHTMessage.MsgType.REPLICATE, myPort), replica);
        }
    }

    private String[] getReplicas(String node) {
        String rep1 = joinedNodes.get(node).successor;
        String rep2 = joinedNodes.get(rep1).successor;
        return new String[]{rep1, rep2};
    }

    private String sendMessageSync(DHTMessage msg, String send_to) throws IOException {
        String reply;
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(send_to));
        ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
        os.writeObject(msg);
        reply = in.readLine();
        socket.close();
        return reply;
    }

    private void sendMessage(DHTMessage dhtMessage, String sendTo) {
        DHTMessage[] messages = new DHTMessage[1];
        messages[0] = dhtMessage;
        Socket socket;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(sendTo));
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            os.writeObject(messages[0]);
            socket.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private String findCorrectNode(String key) {
        for (String node : joinedNodes.keySet()) {
            if (isCorrectNode(key, node)) {
                return node;
            }
        }
        return null;
    }

    private void loge(String m) {
        Log.e(TAG, m);
    }

}
