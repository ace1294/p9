/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaJoin.HJoin;
import gammaSupport.BMap;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

/**
 *
 * @author Jason
 */
public class BFilter extends Thread {
    private WriteEnd out;
	private ReadEnd inB;
    private ReadEnd inM;
    private int joinKey;
    private BMap mapStore;
    
    BFilter(Connector in1, Connector in2, Connector out, int jKey) throws Exception {
    	this.joinKey = jKey;
    	this.out = out.getWriteEnd();
    	this.inB = in1.getReadEnd();
        this.inM = in2.getReadEnd();
        
        out.setRelation(in2.getRelation());
        
        ThreadList.add(this);
    }
    
    public void run() {
    	try {
            mapStore = BMap.makeBMap(inM.getNextString());
            Tuple t = inB.getNextTuple();
            while(true){
                if(mapStore.getValue(t.get(joinKey))){
                    out.putNextTuple(t);
                }
                t = inB.getNextTuple();
                if(t == null)
                	break;
            }
            out.close();
        }
        catch (Exception e) {
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
}
