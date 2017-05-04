/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import basicConnector.*;
import gammaSupport.*;

/**
 *
 * @author Jason
 */
public class Bloom extends Thread {
    
    private final ReadEnd  inReadEnd;
    private final WriteEnd outWriteEndStream;
    private final WriteEnd outWriteEndBitMap;
    private final BMap mapStore;
    private final int joinKey;
    
    public Bloom (Connector in, int jKey, Connector outStream, Connector outBitMap) {
        this.inReadEnd = in.getReadEnd();
    	
    	this.outWriteEndStream = outStream.getWriteEnd();
        this.outWriteEndStream.setRelation(in.getRelation());
    	this.outWriteEndBitMap = outBitMap.getWriteEnd();
        this.outWriteEndBitMap.setRelation(Relation.dummy);
        
    	this.joinKey = jKey;
        this.mapStore = BMap.makeBMap();
    	
    	ThreadList.add(this);
    }
    
    /*
    computes a bloom filter, as in notes.  Observe the icon in the middle of the 
    class: the box has a record stream input (on the left) and a record stream 
    output on the right, and also a bitmap output (red).
    
    clear bit map M
    read each A tuple, hash its join key, and mark corresponding bit in M
    output each tuple A
    after all A tuples read, output M
    */
    @Override
    public void run () {
        try {
            Tuple tuple = this.inReadEnd.getNextTuple();
            while (tuple != null) {
                System.out.println("Filling store with " + tuple.get(this.joinKey));
            	this.mapStore.setValue(tuple.get(this.joinKey), true);
            	this.outWriteEndStream.putNextTuple(tuple);
            	tuple = this.inReadEnd.getNextTuple();
            }
            
            this.outWriteEndBitMap.putNextString(this.mapStore.getBloomFilter());
            this.outWriteEndStream.close();
            this.outWriteEndBitMap.close();
        }
        catch (Exception e){
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }	
    
}
