package edu.buffalo.cse.cse486586.simpledynamo.core;

import java.io.Serializable;

/**
 * Basic data structure used to pass messages between different dynamo nodes
 *
 * Created by utsav on 4/1/15.
 */
public class DHTMessage implements Serializable {
    public enum MsgType {
        SET_PREDECESSOR, SET_SUCCESSOR, INSERT, QUERY, QUERY_ALL_LOCAL,
        QUERY_ALL_GLOBAL, DELETE, DELETE_ALL_LOCAL, DELETE_ALL_GLOBAL, JOINED_NODES,
        REPLICATE, DELETE_REPLICA, FAILED_INSERT, DELETE_FAILED_REPLICA, JOIN, FETCH_REPLICA
    }

    public DHTMessage(String msg, MsgType msgType) {
        this.msg = msg;
        this.msgType = msgType;
    }

    public String getMsg() {
        return msg;
    }

    public MsgType getMsgType() {
        return msgType;
    }

    private String msg;
    private MsgType msgType;

    @Override
    public String toString() {
        return ("msgtype:") + (msgType.toString()) + ("\n") + ("msg:" + msg + "\n");
    }
}
