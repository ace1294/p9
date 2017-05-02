/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

/**
 *
 * @author Jason
 */
public class BFilter extends Thread {
    private final WriteEnd outWriteEnd;
    private final ReadEnd inB;
    private final ReadEnd inM;
    private final int joinKey;
    private BMap mapStore;
    
    BFilter(Connector in1, Connector in2, int jKey, Connector out) throws Exception {
    	this.joinKey = jKey;
    	this.outWriteEnd = out.getWriteEnd();
    	this.inB = in1.getReadEnd();
        this.inM = in2.getReadEnd();
        
        out.setRelation(in2.getRelation());
        
        ThreadList.add(this);
    }
    
    @Override
    public void run() {
    	try {
            this.mapStore = BMap.makeBMap(this.inM.getNextString());
            Tuple t = this.inB.getNextTuple();
            while(true){
                if(this.mapStore.getValue(t.get(this.joinKey))){
                    this.outWriteEnd.putNextTuple(t);
                }
                t = this.inB.getNextTuple();
                if(t == null)
                	break;
            }
            this.outWriteEnd.close();
        }
        catch (Exception e) {
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
}
