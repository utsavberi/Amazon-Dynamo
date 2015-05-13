package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

class NodePair implements Serializable {

    String predecessor, successor;

    public NodePair(String predecessor, String successor) {
        this.predecessor = predecessor;
        this.successor = successor;


    }

    @Override
    public String toString() {
        return predecessor + ":" + successor;
    }


}

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    static final int SERVER_PORT = 10000;
    String myPort;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    //port-><pre,succ>
    HashMap<String, NodePair> joinedNodes = new HashMap<>();

    private void sendMesage(DHTMessage dhtMessage, String sendTo) throws IOException {
        Log.d(TAG, "sending msg to " + sendTo);
        DHTMessage[] msgs = new DHTMessage[1];
        msgs[0] = dhtMessage;
        Socket socket;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(sendTo));
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            Log.d(TAG, "DHTmsg : sending msg " + msgs[0]);
            os.writeObject(msgs[0]);
            Log.d(TAG, "DHTmsg : msg sent to " + sendTo);
            socket.close();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            Log.e(TAG, e1.getLocalizedMessage());
            Log.e(TAG, "msg failed to send.. UnknownHostException");
        }
    }

    private boolean isCorrectNode(String key) {
        if (joinedNodes.size() == 1) {
            Log.d(TAG, "successor is empty or pred == myport returning true");
            return true;
        }
        return isCorrectNode(key, myPort);
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

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to delete" + myPort + ":" + selection);
        switch (selection) {
            case "\"@\"":
                Log.d(TAG, "in @ query");
                for (File file : getContext().getFilesDir().listFiles()) {
                    getContext().deleteFile(file.getName());
                    sendDeleteReplicaMsg(file.getName());
                }
                break;
            case "\"*\"":
                Log.d(TAG, "in * query");
                for (File file : getContext().getFilesDir().listFiles()) {
                    getContext().deleteFile(file.getName());
                    sendDeleteReplicaMsg(file.getName());
                }
                DHTMessage dhtmsg = new DHTMessage();
                dhtmsg.from_port = myPort;
                dhtmsg.msgType = DHTMessage.MsgType.DELETE_ALL;
                broadcastDeleteAll();
                break;
            //selection !=@ && != *
            default:
                if (isCorrectNode(selection)) {
                    Log.d(TAG, "correct node");
                    getContext().deleteFile(selection);
                    sendDeleteReplicaMsg(selection);
                } else {
                    String correctNode = findCorrectNode(selection);
                    Log.d(TAG, "sending to successor");
                    DHTMessage queryMsg = new DHTMessage();
                    queryMsg.msg = selection;
                    queryMsg.msgType = DHTMessage.MsgType.DELETE;
                    try {
                        sendMesage(queryMsg, correctNode);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                break;
        }
        return 0;
    }

    private void broadcastDeleteAll() {
        if (joinedNodes.size() > 1) {
            for (String node : joinedNodes.keySet()) {
                if (!node.equals(myPort)) {
                    DHTMessage msg = new DHTMessage();
                    msg.msgType = DHTMessage.MsgType.DELETE;
                    msg.msg = "\"@\"";
                    try {
                        sendMesage(msg, node);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void sendDeleteReplicaMsg(String fileName) {
        Log.d(TAG, "send delete replicate msg:" + fileName);
        String rep1 = joinedNodes.get(myPort).successor;
        log("rep1" + rep1);
        log("joined nodes" + joinedNodes);
        log("rep1 successor" + joinedNodes.get(rep1));
        String rep2 = joinedNodes.get(rep1).successor;

        try {
            DHTMessage dhtmsg = new DHTMessage();
            dhtmsg.msg = fileName;
            dhtmsg.msgType = DHTMessage.MsgType.DELETE_REPLICA;
            sendMesage(dhtmsg, rep1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            DHTMessage dhtmsg2 = new DHTMessage();
            dhtmsg2.msg = fileName;
            dhtmsg2.msgType = DHTMessage.MsgType.DELETE_REPLICA;
            sendMesage(dhtmsg2, rep2);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(TAG, "inserting value" + values);
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to insert" + myPort);
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        if (isCorrectNode(key)) {
            Log.d(TAG, "CorrectNode");
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
                sendReplicateMsg(MyUtils.cvtoString(values), myPort);
            } catch (Exception e) {
                Log.e(TAG, "File write failed" + e.getLocalizedMessage());
                e.printStackTrace();
            }
        } else {
            Log.d(TAG, "not correctNode");
            String correctNode = findCorrectNode(key);
            DHTMessage insertDhtMessage = new DHTMessage();
            insertDhtMessage.msgType = DHTMessage.MsgType.REPLICATE;
            insertDhtMessage.from_port = myPort;
            insertDhtMessage.msg = MyUtils.cvtoString(values);
            try {
                log("sending insery msg to " + correctNode);
                sendMesage(insertDhtMessage, correctNode);
            } catch (Exception e) {
                log("socket fail, replicating");

                e.printStackTrace();
            }
            sendReplicateMsg(MyUtils.cvtoString(values), correctNode);

        }
        return uri;
    }

    private String findCorrectNode(String key) {
        for (String node : joinedNodes.keySet()) {
            if (isCorrectNode(key, node)) {
                log("found correct node " + node);
                return node;
            }
        }
        log("could not find correct node");
        return null;
    }

    static void log(String m) {
        Log.d(TAG, m);
    }

    static void loge(String m) {
        Log.e(TAG, m);
    }

    private String[] getReplica(String node) {
        String rep1 = joinedNodes.get(node).successor;
        String rep2 = joinedNodes.get(rep1).successor;
        log("replicas for " + node + "are at " + rep1 + " " + rep2);
        return new String[]{rep1, rep2};
    }

    private void sendReplica(String msg, String sendto) {
        DHTMessage dhtmsg = new DHTMessage();
        try {
            dhtmsg.msg = msg;
            dhtmsg.msgType = DHTMessage.MsgType.REPLICATE;
            sendMesage(dhtmsg, sendto);
        } catch (IOException e) {
            loge("failed to send to replica" + sendto);
            e.printStackTrace();
        }
    }

    private void sendReplicateMsg(String msg, String node) {
        String[] replicas = getReplica(node);
        Log.d(TAG, "send replicate msg:" + msg);
        String rep1 = replicas[0];
        String rep2 = replicas[1];
        sendReplica(msg, rep1);
        sendReplica(msg, rep2);
    }

    private String queryAndWaitForReply(String remotePort, DHTMessage dhtMsg) throws IOException {
        Socket socket;
        Log.d(TAG, "inquery and wait for reply " + remotePort + "  " + dhtMsg);
        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(remotePort));
        ObjectOutputStream ois = new ObjectOutputStream(socket.getOutputStream());
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
        ois.writeObject(dhtMsg);
        Log.d(TAG, "msg sent to successor waiting for reply");
        String reply = in.readLine();
        Log.d(TAG, "rcvd reply " + reply);
        socket.close();
        return reply;
    }

    @Override
    public boolean onCreate() {
        log("create started");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        joinedNodes.put("11108", new NodePair("11112", "11116"));
        joinedNodes.put("11120", new NodePair("11116", "11124"));
        joinedNodes.put("11112", new NodePair("11124", "11108"));
        joinedNodes.put("11116", new NodePair("11108", "11120"));
        joinedNodes.put("11124", new NodePair("11120", "11112"));
        //11108=11112:11116,
        // 11120=11116:11124,
        // 11112=11124:11108,
        // 11116=11108:11120,
        // 11124=11120:11112


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getLocalizedMessage());
        }

        new AsyncTask<Void, Void, Void>() {
            @Override
            protected Void doInBackground(Void... params) {
                recoverFailedMsgs();
                return null;
            }
        }.execute();

        log("create ended");
        return false;
    }

    private void recoverFailedMsgs() {
        log("in recover failed msg");
        MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});
        String[] node = getReplica(myPort);

        DHTMessage queryMsg = new DHTMessage();
        queryMsg.msgType = DHTMessage.MsgType.QUERY;
        queryMsg.msg = "\"@\"";
        String send_to = node[0];
        log("sending query @ to sucessor " + node[0]);
        try {
            MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(queryMsg, send_to));
        } catch (IOException e) {
            e.printStackTrace();
        }


        DHTMessage queryMsg2 = new DHTMessage();
        queryMsg2.msgType = DHTMessage.MsgType.QUERY;
        queryMsg2.msg = "\"@\"";
        send_to = joinedNodes.get(myPort).predecessor;
        log("sending query @ to pre " + send_to);
        try {
            MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(queryMsg2, send_to));
        } catch (IOException e) {
            e.printStackTrace();
        }


        DHTMessage queryMsg3 = new DHTMessage();
        queryMsg3.msgType = DHTMessage.MsgType.QUERY;
        queryMsg3.msg = "\"@\"";
        send_to = joinedNodes.get(send_to).predecessor;
        log("sending query @ to pre2 " + send_to);
        try {
            MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(queryMsg3, send_to));
        } catch (IOException e) {
            e.printStackTrace();
        }

        log("got reply.. " + MyUtils.cursorToString(cur) + "inserting");
        cur.moveToFirst();
        while (!cur.isAfterLast()) {
            log("checking corret noce for " + cur.getString(0));
            String pre1 = joinedNodes.get(myPort).predecessor;
            String pre2 = joinedNodes.get(pre1).predecessor;
            if (isCorrectNode(cur.getString(0)) || isCorrectNode(cur.getString(0), pre1) || isCorrectNode(cur.getString(0), pre2)) {

                try {
                    MyUtils.writeToFile(getContext().openFileOutput(cur.getString(0), Context.MODE_PRIVATE), cur.getString(1));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                DHTMessage delMsg = new DHTMessage();
                delMsg.msgType = DHTMessage.MsgType.DELETE_FAILED_REPLICA;
                delMsg.msg = cur.getString(0);
            }
            cur.moveToNext();
        }
        log("recovery done");
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to query" + myPort + ":" + selection);
        MatrixCursor cur;
        cur = new MatrixCursor(new String[]{"key", "value"});
        Log.d(TAG, "started querying");
        switch (selection) {
            case "\"@\"":
                Log.d(TAG, "in @ query");
                for (File file : getContext().getFilesDir().listFiles()) {
                    try {
                        cur.addRow(new Object[]{file.getName(), MyUtils.readFile(getContext().openFileInput(file.getName()))});
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                Log.d(TAG, "final cursor " + cur);
                return cur;
            case "\"*\"":
                Log.d(TAG, "in * query");
                for (File file : getContext().getFilesDir().listFiles()) {
                    try {
                        cur.addRow(new Object[]{file.getName(), MyUtils.readFile(getContext().openFileInput(file.getName()))});
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                MyUtils.appendCurToCur(cur, queryAllBroadcast());
                cur.moveToFirst();
                while (!cur.isAfterLast()) {
                    log(cur.getString(0) + ">>" + cur.getString(1));
                    cur.moveToNext();

                }
                return cur;

            //selection !=@ && != *
            default:
                if (isCorrectNode(selection)) {
                    Log.d(TAG, "correct node");
                    try {
                        cur.addRow(new Object[]{selection, MyUtils.readFile(getContext().openFileInput(selection))});
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    return cur;
                } else {
                    Log.d(TAG, "sending to successor");
                    String correctNode = findCorrectNode(selection);
                    DHTMessage queryMsg = new DHTMessage();
                    queryMsg.msg = selection;
                    queryMsg.msgType = DHTMessage.MsgType.QUERY;
                    try {
                        MyUtils.convertAndAppendToCur(cur, queryAndWaitForReply(correctNode, queryMsg));
                        log("cur debug" + cur.getCount());
                        if (cur.getCount() == 0) throw new IOException();
                    } catch (IOException e) {
                        loge("failed to send query to correct node, trying to fetch from replica");
                        String[] replica = getReplica(correctNode);
                        if (replica[0].equals(myPort)) {
                            try {
                                cur.addRow(new Object[]{selection, MyUtils.readFile(getContext().openFileInput(selection))});
                                loge("fetch done from replica");
                            } catch (IOException e1) {
                                loge("failed to fetch from replica 1 also");

                                e1.printStackTrace();
                            }
                        } else {
                            loge(" trying to fetch from replica[0] " + replica[0]);
                            DHTMessage dhtMessage = new DHTMessage();
                            dhtMessage.msg = selection;
                            dhtMessage.msgType = DHTMessage.MsgType.FETCH_REPLICA;
                            try {
                                MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(dhtMessage, replica[0]));
                                loge("fetch done from replica");
                            } catch (IOException e1) {
                                e1.printStackTrace();
                                loge("failed to fetch from replica 0 trying from replica 1 " + replica[1]);
                                DHTMessage dhtMessage2 = new DHTMessage();
                                dhtMessage2.msg = selection;
                                dhtMessage2.msgType = DHTMessage.MsgType.FETCH_REPLICA;
                                try {
                                    MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(dhtMessage2, replica[1]));
                                    loge("fetch done from replica 1");
                                } catch (IOException e2) {
                                    e2.printStackTrace();
                                    loge("just cant do it");
                                }

                            }
                        }
                        e.printStackTrace();
                    }
                    return cur;

                }

        }
    }

    private MatrixCursor queryAllBroadcast() {
        MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});
        if (joinedNodes.size() > 1) {
            for (String node : joinedNodes.keySet()) {
                if (node.equals(myPort)) continue;
                DHTMessage msg = new DHTMessage();
                msg.msgType = DHTMessage.MsgType.QUERY;
                msg.msg = "\"@\"";
                try {
                    MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(msg, node));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        return cur;
    }

    static String sendAndWaitforReply(DHTMessage msg, String send_to) throws IOException {
        String reply;
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(send_to));
        ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
        os.writeObject(msg);
        reply = in.readLine();
        log("got reply from " + send_to);
        if (reply != null) {
            log(reply);
        }
        socket.close();
        return reply;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
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
                            Log.d(TAG, "received msg" + dhtMessage);
                            switch (dhtMessage.msgType) {
                                case INSERT:
                                    ContentValues values = MyUtils.stringToCv(dhtMessage.msg);
                                    String key = values.getAsString("key");
                                    String string = values.getAsString("value");
                                    Log.d(TAG, "Insert rcvd:" + key + ":" + string);
                                    insert(null, MyUtils.stringToCv(dhtMessage.msg));
                                    break;
                                case QUERY:
                                    Log.d(TAG, "rcvd query msg, fetching cursor");
                                    Cursor cur = query(null, null, dhtMessage.msg, null, null);
                                    Log.d(TAG, "cursor filled converting to string");
                                    String repQuery = MyUtils.cursorToString(cur);
                                    Log.d(TAG, "converted to string " + repQuery);
                                    Log.d(TAG, "replying");
                                    out.println(repQuery);
                                    Log.d(TAG, "reply done ");
                                    break;
                                case DELETE:
                                    delete(null, dhtMessage.msg, null);
                                    break;
                                case REPLICATE:
                                    ContentValues values2 = MyUtils.stringToCv(dhtMessage.msg);
                                    String key1 = values2.getAsString("key");
                                    String value1 = values2.getAsString("value");
                                    FileOutputStream outputStream;
                                    try {
                                        outputStream = getContext().openFileOutput(key1, Context.MODE_PRIVATE);
                                        outputStream.write(value1.getBytes());
                                        outputStream.close();

                                    } catch (Exception e) {
                                        Log.e(TAG, "File write failed");
                                    }
                                    break;
                                case FETCH_REPLICA:
                                    MatrixCursor cur2;
                                    cur2 = new MatrixCursor(new String[]{"key", "value"});
                                    cur2.addRow(new Object[]{dhtMessage.msg,
                                            MyUtils.readFile(getContext().openFileInput(dhtMessage.msg))});
                                    String repFetchQuery = MyUtils.cursorToString(cur2);
                                    Log.d(TAG, "converted to string " + repFetchQuery);
                                    Log.d(TAG, "replying");
                                    out.println(repFetchQuery);
                                    Log.d(TAG, "reply done ");

                                    break;
                                case DELETE_REPLICA:
                                    log("deleting replica" + dhtMessage.msg);
                                    getContext().deleteFile(dhtMessage.msg);
                                    log("done deleting");
                                    break;
                                default:
                                    Log.d(TAG, "rcvd illegal msg type");
                                    break;
                            }
                        } else {
                            Log.d(TAG, "received illegel object");
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
}
