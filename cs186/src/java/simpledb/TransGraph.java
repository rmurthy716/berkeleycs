package simpledb;

import java.util.HashMap;
import java.util.ArrayList;

public class TransGraph {

    private class TransNode {

	private TransactionId tid;
	private ArrayList<TransNode> edges;
	private int pre;
	private int post;
	private boolean visited;

	TransNode(TransactionId tid, TransNode next) {
	    this.tid = tid;
	    edges = new ArrayList<TransNode>();
	    edges.add(next);
	    this.pre = 0;
	    this.post = 0;
	}

	public void zero() {
	    this.pre = 0;
	    this.post = 0;
	}

	public void preupdate() {
	    this.pre = TransGraph.count++;
	}

	public void postupdate() {
	    this.pre = TransGraph.count++;
	}
    }

    private ArrayList<TransNode> nodes;
    public static int count = 0;

    TransGraph() {
	this.nodes = new ArrayList<TransNode>();
    }

    public static synchronized void resetcount() {
	TransGraph.count = 0;
    }
}
