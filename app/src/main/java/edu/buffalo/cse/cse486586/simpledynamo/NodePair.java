package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by utsav on 1/27/16.
 */
public class NodePair {
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
