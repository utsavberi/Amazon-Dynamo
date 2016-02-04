package edu.buffalo.cse.cse486586.simpledynamo.core;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse.cse486586.simpledynamo.repositories.KeyValueRepository;

/**
 * A distributed Hash Table with Key-Value based storage and replication
 * for failure handling and recovering
 *
 * Created by utsav on 1/27/16.
 */
public class Dynamo {

    private final int SERVER_PORT;
    private final String TAG = "Dynamo";
    private final String myPort;
    private HashMap<String, NodePair> joinedNodes = new HashMap<>();
    private KeyValueRepository repository;

    /**
     * Constructs a new Instance of the Dynamo on the given port
     *
     * @param serverPort on which the dynamo Message Manager Service will be started
     * @param localPort  the local port number used to identify the current node
     * @param repository used for storing and reading data
     */
    public Dynamo(int serverPort, int localPort, KeyValueRepository repository) {
        this.SERVER_PORT = serverPort;
        this.repository = repository;
        myPort = String.valueOf(localPort);
    }

    /**
     * Starts the dynamo service on the port specified while creating an instance of dynamo.
     */
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

    /**
     * Adds a new node in the ring based hash table and also specifies the next and previous node
     *
     * @param node new node to be added
     * @param prev node previous to the new node
     * @param next node next to the new node
     */
    public void addNode(String node, String prev, String next) {
        joinedNodes.put(node, new NodePair(prev, next));
    }

    /**
     * Associates the specified value with the specified key in this map.
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        String values = key + "," + value;
        if (isCorrectNode(key)) {
            repository.store(key,value);
            sendReplicateMsg(values, myPort);
        } else {
            String correctNode = findCorrectNode(key);
            DHTMessage insertDhtMessage = new DHTMessage(values, DHTMessage.MsgType.REPLICATE);
            sendMessage(insertDhtMessage, correctNode);
            sendReplicateMsg(values, correctNode);
        }
    }

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains
     * no mapping for the key
     * @param key
     * @return
     */
    public String getValue(String key) {
        if (isCorrectNode(key)) {
            return repository.fetch(key);
        } else {
            Log.d(TAG, "sending to successor");
            String correctNode = findCorrectNode(key);
            DHTMessage queryMsg = new DHTMessage(key, DHTMessage.MsgType.QUERY);
            try {
                return sendMessageSync(queryMsg, correctNode);
            } catch (IOException e) {
                loge("failed to send query to correct node, trying to fetch from replica");
                e.printStackTrace();
                return fetchFromReplica(key, correctNode);
            }
        }
    }

    /**
     * Returns all key value pairs stored in the current node
     *
     * @return
     */
    public HashMap<String, String> getAllLocal() {
        return repository.fetchAll();
    }

    /**
     * Returns all the key value pairs stored in all the nodes
     *
     * @return
     */
    public HashMap<String, String> getAllGlobal() {
        HashMap<String, String> result = new HashMap<>();
        result.putAll(repository.fetchAll());
        result.putAll(queryAllBroadcast());
        return result;
    }

    /**
     * Removes the mapping for the specified key from local node if present else forwards
     * the request to the correct node
     * @param key
     */
    public void remove(String key) {
        if (isCorrectNode(key)) {
            Log.d(TAG, "correct node");
            repository.remove(key);
            sendDeleteReplicaMsg(key);
        } else {
            String correctNode = findCorrectNode(key);
            Log.d(TAG, "sending to correct Node");
            DHTMessage queryMsg = new DHTMessage(key, DHTMessage.MsgType.DELETE);
            sendMessage(queryMsg, correctNode);
        }
    }

    /**
     * Removes all the key value pairs stored in the local node
     */
    public void removeAllLocal() {
        HashMap<String, String> all = repository.fetchAll();
        for (String file : all.keySet()) {
            sendDeleteReplicaMsg(file);
        }
        repository.removeAll();
    }

    /**
     * Removes all the key value pairs stored in all the nodes
     */
    public void removeAllGlobal() {
        removeAllLocal();
        for (String node : joinedNodes.keySet()) {
            if (!node.equals(myPort)) {
                DHTMessage msg = new DHTMessage("", DHTMessage.MsgType.DELETE_ALL_LOCAL);
                sendMessage(msg, node);
            }
        }
    }

    /**
     * Stores a replica in the current node
     * @param key
     * @param value
     */
    public void writeReplica(String key, String value) {
        repository.store(key, value);
    }

    /**
     * Fetches a replica associated with the key from the current node
     *
     * @param key
     * @return
     */
    public String readReplica(String key) {
        return repository.fetch(key);
    }

    /**
     * Removes a replica from the current node
     * @param key
     */
    public void deleteReplica(String key) {
        repository.remove(key);
    }

    private String fetchFromReplica(String key, String correctNode) {
        String[] replica = getReplicas(correctNode);
        if (replica[0].equals(myPort) || replica[1].equals(myPort)) {
            return repository.fetch(key);
        } else {
            loge(" trying to fetch from replica[0] " + replica[0]);
            DHTMessage dhtMessage = new DHTMessage(key, DHTMessage.MsgType.FETCH_REPLICA);
            try {
                return sendMessageSync(dhtMessage, replica[0]);
            } catch (IOException e1) {
                e1.printStackTrace();
                loge("failed to fetch from replica 0 trying from replica 1 " + replica[1]);
                DHTMessage dhtMessage2 = new DHTMessage(key, DHTMessage.MsgType.FETCH_REPLICA);
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
        DHTMessage queryMsg = new DHTMessage("", DHTMessage.MsgType.QUERY_ALL_LOCAL);
        for (String replica : getReplicas(myPort)) {
            try {
                result.putAll(convertToHashMap(sendMessageSync(queryMsg, replica)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (String replicatedFrom : getNodesReplicatedFrom(myPort)) {
            try {
                result.putAll(convertToHashMap(sendMessageSync(queryMsg, replicatedFrom)));
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
                repository.store(key,value);
            }
        }
    }

    private void sendDeleteReplicaMsg(String fileName) {
        DHTMessage dhtmsg = new DHTMessage(fileName, DHTMessage.MsgType.DELETE_REPLICA);
        for (String replica : getReplicas(myPort)) {
            sendMessage(dhtmsg, replica);
        }
    }

    private HashMap<String, String> queryAllBroadcast() {
        HashMap<String, String> result = new HashMap<>();
        if (joinedNodes.size() > 1) {
            for (String node : joinedNodes.keySet()) {
                if (node.equals(myPort)) continue;
                DHTMessage msg = new DHTMessage("", DHTMessage.MsgType.QUERY_ALL_LOCAL);
                try {
                    result.putAll(convertToHashMap(sendMessageSync(msg, node)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    private HashMap<String, String> convertToHashMap(String s) {
        HashMap<String, String> result = new HashMap<>();
        if (s == null || s.trim().isEmpty()) return result;
        String[] rows = s.split("\\|");
        for (String row : rows) {
            if (row.trim().isEmpty()) continue;
            String[] keyValue = row.split(",");
            result.put(keyValue[0], keyValue[1]);
        }
        return result;
    }
    private boolean isCorrectNode(String key) {
        return joinedNodes.size() == 1 || isCorrectNode(key, myPort);
    }

    private boolean isCorrectNode(String key, String node) {
        String pred_port = joinedNodes.get(node).predecessor;
        try {
            String id = genHash(key),
                    pred_id = genHash(getNodeIdFromPort(pred_port)),
                    my_id = genHash(getNodeIdFromPort(node));
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

    private String getNodeIdFromPort(String port) {
        return Integer.toString(Integer.parseInt(port) / 2);
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

    private void sendReplicateMsg(String msg, String node) {
        for (String replica : getReplicas(node)) {
            sendMessage(new DHTMessage(msg, DHTMessage.MsgType.REPLICATE), replica);
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

    public static class NodePair {
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
}
