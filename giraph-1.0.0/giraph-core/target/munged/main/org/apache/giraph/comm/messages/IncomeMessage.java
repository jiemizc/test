package org.apache.giraph.comm.messages;

import org.apache.hadoop.io.Writable;

/**
 * Ensure to use message type is IncomeMessage when using incoming mode.
 * Send income message will broadcast the vertexValue to all workers, 
 * and edgeValue is null before received by server.
 * Server will fill the edgeValue looking up its own outEdgeMap
 * 
 * @author zhouchang
 *
 * @param <M>
 * @param <E>
 */
public abstract class IncomeMessage<M extends Writable, E extends Writable> implements Writable{
	M incomeVertexMsg;
	E incomeEdgeValue;
	public M getIncomeVertexMsg(){
		return incomeVertexMsg;
	}
	public E getIncomeEdgeValue(){
		return incomeEdgeValue;
	}
	public void setIncomeVertexMsg(M msg){
		incomeVertexMsg = msg;
	}
	public void setIncomeEdgeValue(Writable edgeValue){
		E ev = (E) edgeValue;
		incomeEdgeValue = ev;
	}
}
