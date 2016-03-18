package edu.pku.db.mocgraph.example.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class IntArrayWritable implements Writable{
	public List<Integer> id;
	public IntArrayWritable(List<Integer> msg) {
		this.id = msg;
	}
	public IntArrayWritable(){
		id = new ArrayList<Integer>();
	};
	public IntArrayWritable(int size){
		id = new ArrayList<Integer>(size);
	};
	public IntArrayWritable(IntArrayWritable other){
		id = new ArrayList<Integer>();
		for(Integer tmp:other.id){
			id.add(tmp);
		}
	}
	public void add(Integer i){
		id.add(i);
	}
	public int getSize(){
		return id.size();
	}
	public int get(int index){
		return id.get(index);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id.size());
		for(int tmp: id)
			out.writeInt(tmp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		if(id!=null)
			id.clear();
		for(int i=0; i<len; i++)
			id.add(in.readInt());
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(Integer i:id){
			sb.append(String.valueOf(i)+" ");
		}
		return sb.toString();
	}
}
