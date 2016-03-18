package edu.pku.db.mocgraph.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;


public class LandmarkTable implements Writable {
	Map<Integer, PairIntWritable> table = new HashMap<Integer, PairIntWritable>();
	
	public void clear(){
		table.clear();
	}
	public Map<Integer, PairIntWritable> getTable() {
		return table;
	}
	public void setTable(Map<Integer, PairIntWritable> table) {
		this.table = table;
	}
	public boolean contains(Integer index){
		return table.containsKey(index);
	}
	public Integer getPre(Integer key){
		if(table.containsKey(key)){
			return table.get(key).getPre();
		}else
			return null;
	}
	public Integer getCost(Integer key){
		PairIntWritable res = table.get(key);
		if(res == null)
			return null;
		else
			return res.getCost();
	}
	public Set<Entry<Integer, PairIntWritable>> entrySet(){
		return table.entrySet();
	}
	public void put(Integer key, Integer pre, Integer cost){
		PairIntWritable value = table.get(key);
		if(value!=null){
			value.setCost(cost);
			value.setPre(pre);
		}else{
			table.put(key.intValue(), new PairIntWritable(pre.intValue(), cost.intValue()));
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		table.clear();
		int size = in.readInt();
		for(int i=0; i<size; i++){
			int landmark = in.readInt();
			PairIntWritable t = new PairIntWritable();
			t.readFields(in);
			table.put(landmark, t);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(table.size());
	    for (Map.Entry<Integer, PairIntWritable> t: table.entrySet()) {
	      out.writeInt(t.getKey());
	      t.getValue().write(out);
	    }
	}
	
	@Override	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(Entry<Integer, PairIntWritable> entry : table.entrySet()){
			sb.append("[landmarkId="+entry.getKey()+", pre="+
					entry.getValue().getPre()+
					", cost="+entry.getValue().getCost()+"]");
		}
		return sb.toString();
	}
}
