/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gamma;

import basicConnector.Connector;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;

/**
 *
 * @author Jason
 */
public class BloomSimulator extends Thread {
    private final BMap mapStore;
    private final WriteEnd outWriteEndBitMap;
    
    public BloomSimulator(Connector outBitMap) {
        
        this.outWriteEndBitMap = outBitMap.getWriteEnd();
        this.outWriteEndBitMap.setRelation(Relation.dummy);
        this.mapStore = BMap.makeBMap();
        
        ThreadList.add(this);
    }
    
    @Override
    public void run () {
        try {
            this.outWriteEndBitMap.putNextString(this.mapStore.getBloomFilter());
            this.outWriteEndBitMap.close();
        }
        catch(Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
