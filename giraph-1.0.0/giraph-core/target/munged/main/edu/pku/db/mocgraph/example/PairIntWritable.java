package edu.pku.db.mocgraph.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairIntWritable implements Writable{
	int pre;
	int cost;
	public boolean isUpdated;
	public int getPre() {
		return pre;
	}
	public void setPre(int pre) {
		this.pre = pre;
	}
	public int getCost() {
		return cost;
	}
	public void setCost(int cost) {
		this.cost = cost;
	}
	public PairIntWritable(){
		
	}
	public PairIntWritable(int pre, int cost){
		this.pre = pre;
		this.cost = cost;
	}
	
	@Override
	public String toString(){
		return "[pre="+pre+", cost="+cost+"]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		pre = in.readInt();
		cost = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(pre);
		out.writeInt(cost);
	}

}
