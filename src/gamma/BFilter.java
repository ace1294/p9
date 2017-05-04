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
public class BFilter extends Thread {
    
    private final ReadEnd inReadEndBitMap;
    private final ReadEnd inReadEndB;
    private final int joinKey;
    private final WriteEnd outWriteEnd;
    
    private BMap mapStore;
    
    public BFilter (Connector inBitMap, Connector inBStream, int jKey, Connector out) throws Exception {
    	this.inReadEndBitMap = inBitMap.getReadEnd();
        this.inReadEndB = inBStream.getReadEnd();
        this.joinKey = jKey;
    	this.outWriteEnd = out.getWriteEnd();
        
        out.setRelation(inBStream.getRelation());
        
        ThreadList.add(this);
    }
    
    /*s
    read bit map M
    read each tuple of B, hash its join key: if corresponding bit in M is not set 
    discard tuple as it will never join with A tuples
    else output tuple
    */
    @Override
    public void run() {
    	try {
            this.mapStore = BMap.makeBMap(this.inReadEndBitMap.getNextString());
            Tuple tuple = this.inReadEndB.getNextTuple();
            int count = 0;
            while (tuple != null) {
                System.out.println("Bstream value try to filter " + tuple.get(this.joinKey) + " Value from Map " + this.mapStore.getValue(tuple.get(this.joinKey)));
                if (this.mapStore.getValue(tuple.get(this.joinKey))) {
                    this.outWriteEnd.putNextTuple(tuple);
                }
                tuple = this.inReadEndB.getNextTuple();
                count++;
            }
            this.outWriteEnd.close();
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
    
}
