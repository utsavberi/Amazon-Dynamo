package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by utsav on 4/1/15.
 */
public class DHTMessage implements Serializable {
    public enum MsgType {
        SET_PREDECESSOR, SET_SUCCESSOR, INSERT, QUERY, QUERY_ALL_LOCAL,
        QUERY_ALL_GLOBAL, DELETE, DELETE_ALL_LOCAL, DELETE_ALL_GLOBAL, JOINED_NODES,
        REPLICATE, DELETE_REPLICA, FAILED_INSERT, DELETE_FAILED_REPLICA, JOIN, FETCH_REPLICA
    }

    public DHTMessage(String msg, MsgType msgType, String from_port) {
        this.msg = msg;
        this.msgType = msgType;
        this.fromPort = from_port;
    }

    public String getMsg() {
        return msg;
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public String getFromPort() {
        return fromPort;
    }

    private String msg;
    private MsgType msgType;
    private String fromPort;

    @Override
    public String toString() {
        return ("msgtype:") + (msgType.toString()) + ("\n") +
                ("from port:") + (fromPort) + ("\n") + ("msg:" + msg + "\n");
    }
}
