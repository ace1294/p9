/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.WriteEnd;
import basicConnector.ReadEnd;
import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

/**
 *
 * @author Jason
 */
public class Bloom extends Thread {
    private WriteEnd outA;
    private WriteEnd outM;
    private ReadEnd  readIn;
    private BMap mapStore;
    private int joinKey;
    Bloom (Connector out1, Connector out2, Connector in, int jKey) {
    	this.mapStore = BMap.makeBMap();
    	
    	this.outA = out1.getWriteEnd();
    	this.outM = out2.getWriteEnd();
    	this.outA.setRelation(out1.getRelation());
    	this.outM.setRelation(Relation.dummy);
    	this.joinKey = jKey;
    	this.readIn = in.getReadEnd();
    	
    	ThreadList.add(this);
    	
    }
    
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
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, e);
        }
    }	
    
}
