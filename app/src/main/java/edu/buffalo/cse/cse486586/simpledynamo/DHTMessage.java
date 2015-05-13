package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by utsav on 4/1/15.
 */
public class DHTMessage implements Serializable {
    public String msg;

    public enum MsgType {
        SET_PREDECESSOR, SET_SUCCESSOR, INSERT, QUERY, QUERY_ALL, DELETE, DELETE_ALL, JOINED_NODES,
        REPLICATE, DELETE_REPLICA, FAILED_INSERT, DELETE_FAILED_REPLICA, JOIN, FETCH_REPLICA
    }

    MsgType msgType;
    String from_port;

    @Override
    public String toString() {
        return ("msgtype:") + (msgType.toString()) + ("\n") +
                ("fromport:") + (from_port) + ("\n") + ("msg:" + msg + "\n");
    }
}
