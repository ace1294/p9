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
    
    private final WriteEnd outA;
    private final WriteEnd outM;
    private final ReadEnd  readIn;
    private final BMap mapStore;
    private final int joinKey;
    
    public Bloom (Connector out1, Connector out2, Connector in, int jKey) {
    	this.mapStore = BMap.makeBMap();
    	
    	this.outA = out1.getWriteEnd();
    	this.outM = out2.getWriteEnd();
    	this.outA.setRelation(out1.getRelation());
    	this.outM.setRelation(Relation.dummy);
    	this.joinKey = jKey;
    	this.readIn = in.getReadEnd();
    	
    	ThreadList.add(this);
    }
    
    /*
    computes a bloom filter, as in notes.  Observe the icon in the middle of the 
    class: the box has a record stream input (on the left) and a record stream 
    output on the right, and also a bitmap output (red).
    */
    @Override
    public void run () {
        try {
            Tuple temp = readIn.getNextTuple();
            while(true) {
            	mapStore.setValue(temp.get(joinKey), true);
            	outA.putNextTuple(temp);
            	if((temp = readIn.getNextTuple()) == null)
            		break;
            }
            
            outM.putNextString(mapStore.getBloomFilter());
            
            outA.close();
            outM.close();
        }
        catch (Exception e){
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }	
    
}
