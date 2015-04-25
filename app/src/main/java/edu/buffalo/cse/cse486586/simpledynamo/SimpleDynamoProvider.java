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
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

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
    //    public String predecessor_port = "";
//    public String successor_port = "";
    String myPort;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    //port-><pre,succ>
    HashMap<String, NodePair> joinedNodes = new HashMap();

    public void sendMesage(DHTMessage dhtMessage, String sendTo) {
        Log.d(TAG, "sending msg to " + sendTo);
        dhtMessage.send_to = sendTo;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dhtMessage);
        Log.d(TAG, "msg sent");
    }

    private boolean isCorrectNode(String key) {

//        Log.d(TAG, successor_port + " " + predecessor_port + " myport:" + myPort);

//        if (successor_port.isEmpty() || (predecessor_port.equals(myPort))) {
        if (joinedNodes.size() == 1) {
            Log.d(TAG, "successor is empty or pred == myport returning true");
            return true;
        }
        return isCorrectNode(key, myPort);
//        return false;
    }

    private boolean isCorrectNode(String key, String node) {
        Log.d(TAG, "checking for correct node");
        String pred_port = joinedNodes.get(node).predecessor;
//        Log.d(TAG, successor_port + " " + pred_port + " " + node);

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
//        Log.d(TAG, selection + " " + successor_port + " " + predecessor_port + " " + myPort);
        if (selection.equals("\"@\""))
//        || (selection.equals("\"*\"") &&
//                (successor_port.isEmpty() || (predecessor_port.equals(myPort)))))
        {
            Log.d(TAG, "in @ query");
            for (File file : getContext().getFilesDir().listFiles()) {
                getContext().deleteFile(file.getName());
                sendDeleteReplicaMsg(file.getName());
            }
        } else if (selection.equals("\"*\"")) {
            Log.d(TAG, "in * query");
            for (File file : getContext().getFilesDir().listFiles()) {
                getContext().deleteFile(file.getName());
                sendDeleteReplicaMsg(file.getName());
            }
            DHTMessage dhtmsg = new DHTMessage();
            dhtmsg.from_port = myPort;
            dhtmsg.msgType = DHTMessage.MsgType.DELETE_ALL;
            broadcastDeleteAll();
        }
        //selection !=@ && != *
        else {
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
                sendMesage(queryMsg, correctNode);
            }
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
                    sendMesage(msg, node);
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
        DHTMessage dhtmsg = new DHTMessage();
        dhtmsg.msg = fileName;
        dhtmsg.msgType = DHTMessage.MsgType.DELETE_REPLICA;
        sendMesage(dhtmsg, rep1);
        DHTMessage dhtmsg2 = new DHTMessage();
        dhtmsg2.msg = fileName;
        dhtmsg2.msgType = DHTMessage.MsgType.DELETE_REPLICA;
        sendMesage(dhtmsg2, rep2);

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private void updatePredSucc(String updatedPortNum, String nodeToUpdate, DHTMessage.MsgType msgType) {
        DHTMessage dhtMessage = new DHTMessage();
        dhtMessage.from_port = updatedPortNum;
        dhtMessage.msgType = msgType;
        sendMesage(dhtMessage, nodeToUpdate);
    }

    //only called on central node ie REMOTE_PORT0
    private void nodeJoin(DHTMessage join_dhtMessage) {
        try {
            Log.d(TAG, "hash join" + join_dhtMessage.from_port + ":" + MyUtils.genHash(MyUtils.getNodeIdFromPort(join_dhtMessage.from_port)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String joinReq_from_port = join_dhtMessage.from_port;
//        if(joinedNodes.containsKey(joinReq_from_port)==true)
        try {
            if (joinedNodes.size() == 1) {
                Log.d(TAG, "in joined node size 1");
//                predecessor_port = joinReq_from_port;
//                successor_port = joinReq_from_port;
                joinedNodes.put(REMOTE_PORT0, new NodePair(joinReq_from_port, joinReq_from_port));
                updatePredSucc(REMOTE_PORT0, joinReq_from_port, DHTMessage.MsgType.SET_SUCCESSOR);
                updatePredSucc(REMOTE_PORT0, joinReq_from_port, DHTMessage.MsgType.SET_PREDECESSOR);
                joinedNodes.put(joinReq_from_port, new NodePair(REMOTE_PORT0, REMOTE_PORT0));
            } else {
                String nodePosToInsert = REMOTE_PORT0;
                while (true) {
                    String pre, wanna_join, curr;
                    curr = MyUtils.genHash(MyUtils.getNodeIdFromPort(nodePosToInsert));
                    pre = MyUtils.genHash(MyUtils.getNodeIdFromPort(joinedNodes.get(nodePosToInsert).predecessor));
                    wanna_join = MyUtils.genHash(MyUtils.getNodeIdFromPort(join_dhtMessage.from_port));
                    if ((wanna_join.compareTo(pre) > 0 && wanna_join.compareTo(curr) < 0) ||
                            ((pre.compareTo(curr) > 0) && (wanna_join.compareTo(curr) < 0 ||
                                    wanna_join.compareTo(pre) > 0))) {
                        break;
                    } else nodePosToInsert = joinedNodes.get(nodePosToInsert).successor;
                }

                String nodePos_pre = joinedNodes.get(nodePosToInsert).predecessor;

                updatePredSucc(joinReq_from_port, nodePosToInsert, DHTMessage.MsgType.SET_PREDECESSOR);
                updatePredSucc(joinReq_from_port, nodePos_pre, DHTMessage.MsgType.SET_SUCCESSOR);
                updatePredSucc(nodePosToInsert, joinReq_from_port, DHTMessage.MsgType.SET_SUCCESSOR);
                updatePredSucc(nodePos_pre, joinReq_from_port, DHTMessage.MsgType.SET_PREDECESSOR);


                NodePair update = new NodePair(joinReq_from_port, joinedNodes.get(nodePosToInsert).successor);
                joinedNodes.put(nodePosToInsert, update);
                NodePair update2 = new NodePair(joinedNodes.get(nodePos_pre).predecessor, joinReq_from_port);
                joinedNodes.put(nodePos_pre, update2);
                joinedNodes.put(joinReq_from_port, new NodePair(nodePos_pre, nodePosToInsert));
                broadcastJoinedNodes();
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "joined nodes" + MyUtils.printJoinedNodes(joinedNodes));
//        }
    }

    private void broadcastJoinedNodes() {
        for (Map.Entry<String, NodePair> e : joinedNodes.entrySet()) {
            if (!e.getKey().equals(REMOTE_PORT0)) {
                Log.d(TAG, "JOOIINN" + new DHTMessage() {{
                    msgType = MsgType.JOINED_NODES;
                    msg = joinedNodes.toString();
                }}.toString());
                DHTMessage dhtMessage = new DHTMessage();
                dhtMessage.from_port = "3333";
                dhtMessage.msgType = DHTMessage.MsgType.JOINED_NODES;
                dhtMessage.msg = joinedNodes.toString();

                sendMesage(dhtMessage, e.getKey());
            }
        }
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(TAG, "inserting value" + values);
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to insert" + myPort);
        String key = values.getAsString("key");
        String value = values.getAsString("value");
//        Log.d(TAG, "succc:" + successor_port);
        if (isCorrectNode(key)) {
            Log.d(TAG, "CorrectNode");
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
                sendReplicateMsg(MyUtils.cvtoString(values));
            } catch (Exception e) {
                Log.e(TAG, "File write failed" + e.getLocalizedMessage());
                e.printStackTrace();
            }
        } else {
            Log.d(TAG, "not correctNode");
            String correctNode = findCorrectNode(key);
            DHTMessage insertDhtMessage = new DHTMessage();
            insertDhtMessage.msgType = DHTMessage.MsgType.INSERT;
            insertDhtMessage.from_port = myPort;
            insertDhtMessage.cv_msg = MyUtils.cvtoString(values);
//            sendMesage(insertDhtMessage, successor_port);
            sendMesage(insertDhtMessage, correctNode);

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

    private void sendReplicateMsg(String msg) {
        Log.d(TAG, "send replicate msg:" + msg);
        String rep1 = joinedNodes.get(myPort).successor;
        log("rep1" + rep1);
        log("joined nodes" + joinedNodes);
        log("rep1 successor" + joinedNodes.get(rep1));

        String rep2 = joinedNodes.get(rep1).successor;
        DHTMessage dhtmsg = new DHTMessage();
        dhtmsg.msg = msg;
        dhtmsg.msgType = DHTMessage.MsgType.REPLICATE;
        sendMesage(dhtmsg, rep1);
        DHTMessage dhtmsg2 = new DHTMessage();
        dhtmsg2.msg = msg;
        dhtmsg2.msgType = DHTMessage.MsgType.REPLICATE;
        sendMesage(dhtmsg2, rep2);

    }

    private String queryAndWaitForReply(String remotePort, DHTMessage dhtMsg) {
        Socket socket;
        Log.d(TAG, "inquery and wait for reply " + remotePort + "  " + dhtMsg);
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "returning empty");
        return "";
    }

    @Override
    public boolean onCreate() {
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
//        if (myPort.equals(remotePorts[0])) {
//            successor_port = REMOTE_PORT0;
//            predecessor_port = REMOTE_PORT0;
//            joinedNodes.put(REMOTE_PORT0, new NodePair(REMOTE_PORT0, REMOTE_PORT0));
//
//        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getLocalizedMessage());
        }
//        if (!myPort.equals(REMOTE_PORT0)) {
//            DHTMessage joinMessage = new DHTMessage();
//            joinMessage.from_port = myPort;
//            joinMessage.msgType = DHTMessage.MsgType.JOIN;
//            sendMesage(joinMessage, REMOTE_PORT0);
//        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to query" + myPort + ":" + selection);
        MatrixCursor cur;
        cur = new MatrixCursor(new String[]{"key", "value"});
        Log.d(TAG, "started querying");
//        Log.d(TAG, selection + " " + successor_port + " " + predecessor_port + " " + myPort);
        if (selection.equals("\"@\""))
//        || (selection.equals("\"*\"") &&
//                (successor_port.isEmpty() || (predecessor_port.equals(myPort)))))
        {
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
        } else if (selection.equals("\"*\"")) {
            Log.d(TAG, "in * query");
//            String remotePort = successor_port;
            for (File file : getContext().getFilesDir().listFiles()) {
                try {
                    cur.addRow(new Object[]{file.getName(), MyUtils.readFile(getContext().openFileInput(file.getName()))});
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            MyUtils.appendCurToCur(cur, queryAllBroadcast());
//            DHTMessage dhtmsg = new DHTMessage();
//            dhtmsg.from_port = myPort;
//            dhtmsg.msgType = DHTMessage.MsgType.QUERY_ALL;
//            MyUtils.convertAndAppendToCur(cur, queryAndWaitForReply(remotePort, dhtmsg));
//            Log.d(TAG, "received cursor");
//            Log.d(TAG, MyUtils.cursorToString(cur));
            log("lololo");
            cur.moveToFirst();
            while (!cur.isAfterLast()) {
                log(cur.getString(0) + ">>" + cur.getString(1));
                cur.moveToNext();

            }
            return cur;
        }
        //selection !=@ && != *
        else {
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
                MyUtils.convertAndAppendToCur(cur, queryAndWaitForReply(correctNode, queryMsg));
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
                msg.send_to = node;
                MyUtils.convertAndAppendToCur(cur, sendAndWaitforReply(msg));
//                try {
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                            Integer.parseInt(node));
//                    ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
//                    BufferedReader in =
//                            new BufferedReader(
//                                    new InputStreamReader(socket.getInputStream()));
//                    DHTMessage msg = new DHTMessage();
//                    msg.msgType = DHTMessage.MsgType.QUERY;
//                    msg.msg = "\"@\"";
//                    os.writeObject(msg);
//                    String reply =in.readLine();
//                    MyUtils.convertAndAppendToCur(cur,reply);
//                    socket.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }

            }
        }
        return cur;
    }

    static String sendAndWaitforReply(DHTMessage msg) {
        String reply = null;
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msg.send_to));
            ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
//            DHTMessage msg = new DHTMessage();
//            msg.msgType = DHTMessage.MsgType.QUERY;
//            msg.msg = "\"@\"";
            os.writeObject(msg);
            reply = in.readLine();
            log("got reply from " + msg.send_to);
            log(reply);
//            MyUtils.convertAndAppendToCur(cur,reply);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reply;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }


    private class ClientTask extends AsyncTask<DHTMessage, Void, Void> {
        @Override
        protected Void doInBackground(DHTMessage... msgs) {
            String remotePort = msgs[0].send_to;
            Socket socket;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
                Log.d(TAG, "DHTmsg : sending msg " + msgs[0]);
                os.writeObject(msgs[0]);
                Log.d(TAG, "DHTmsg : msg sent to " + remotePort);
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, e.getLocalizedMessage());
                Log.e(TAG, "retrying");
                try {
                    Thread.sleep(1000);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                    Log.e(TAG, e.getLocalizedMessage());
                    log("msg failed to send.. i give up..");
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
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
                                case JOIN:
                                    nodeJoin(dhtMessage);
                                    break;
                                case JOINED_NODES:
                                    Log.d(TAG, "joined node update" + dhtMessage.msg);
                                    upateJoinedNodes((dhtMessage.msg));
                                    break;
//                                case SET_PREDECESSOR:
//                                    predecessor_port = dhtMessage.from_port;
//                                    break;
//                                case SET_SUCCESSOR:
//                                    successor_port = dhtMessage.from_port;
//                                    break;
                                case INSERT:
                                    ContentValues values = MyUtils.stringToCv(dhtMessage.cv_msg);
                                    String key = values.getAsString("key");
                                    String string = values.getAsString("value");
                                    Log.d(TAG, "Insert rcvd:" + key + ":" + string);
                                    insert(null, MyUtils.stringToCv(dhtMessage.cv_msg));
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
//                                case QUERY_ALL:
//                                    String curr = MyUtils.cursorToString(query(null, null, "\"@\"", null, null));
//                                    if (!dhtMessage.from_port.equals(successor_port)) {
//                                        curr += queryAndWaitForReply(successor_port, dhtMessage);
//                                    } else {
//                                        Log.d(TAG, "reached lastnode");
//                                    }
//                                    out.println(curr);
//                                    break;
                                case DELETE:
                                    delete(null, dhtMessage.msg, null);
                                    break;
//                                case DELETE_ALL:
//                                    delete(null, "\"@\"", null);
//                                    if (!dhtMessage.from_port.equals(successor_port)) {
//                                        sendMesage(dhtMessage, successor_port);
//                                    } else {
//                                        Log.d(TAG, "reached lastnode");
//                                    }
//                                    break;
                                case REPLICATE:
                                    ContentValues values2 = MyUtils.stringToCv(dhtMessage.msg);
                                    String key1 = values2.getAsString("key");
                                    String value1 = values2.getAsString("value");
//                                    Log.d(TAG, "succc:" + successor_port);
//                                        Log.d(TAG, "CorrectNode");
                                    FileOutputStream outputStream;
                                    try {
                                        outputStream = getContext().openFileOutput(key1, Context.MODE_PRIVATE);
                                        outputStream.write(value1.getBytes());
                                        outputStream.close();

                                    } catch (Exception e) {
                                        Log.e(TAG, "File write failed");
                                    }
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

        private void upateJoinedNodes(String msg) {
            HashMap<String, NodePair> tmp = new HashMap<>();
            msg = msg.substring(1, msg.length() - 1).trim();
            Log.d(TAG, "trunc msg" + msg);
            String[] rows = msg.split(",");
            for (String row : rows) {
                String key = row.split("=")[0];
                String value = row.split("=")[1];
                tmp.put(key.trim(), new NodePair(value.split(":")[0].trim(), value.split(":")[1].trim()));
            }
            joinedNodes = tmp;
            Log.d(TAG, "updated joined nodes" + joinedNodes.toString());
        }
    }
}
