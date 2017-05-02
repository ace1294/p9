package gamma;

import basicConnector.*;
import gammaSupport.*;

public class MergeM extends Thread {
    
    private final ReadEnd[] inReadEnds;
    private final WriteEnd outWriteEnd;
    private BMap mapStore;

    public MergeM (Connector[] inputConnectorArray, Connector outputConnectorArray){
    	this.inReadEnds = new ReadEnd[inputConnectorArray.length];
        
        for (int i = 0; i < inputConnectorArray.length; i++) {
            this.inReadEnds[i] = inputConnectorArray[i].getReadEnd();
        }
        
        this.outWriteEnd = outputConnectorArray.getWriteEnd();
        outputConnectorArray.setRelation(Relation.dummy);
        
        this.mapStore = BMap.makeBMap();
        
        ThreadList.add(this);
    }
    
    /*
    merges 4 distinct bitmaps into a single bitmap.
    */
    @Override
    public void run () {
        try {
            int i = 0;
            String tupleString = this.inReadEnds[i].getNextString();
            while (tupleString != null && i < this.inReadEnds.length - 1){
                BMap tempStore = BMap.makeBMap(tupleString);
                mapStore = BMap.or(mapStore, tempStore);
                i++;
                tupleString = this.inReadEnds[i].getNextString();
            }
            
            BMap tempStore = BMap.makeBMap(tupleString);
            this.mapStore = BMap.or(mapStore, tempStore);
            
            this.outWriteEnd.putNextString(mapStore.getBloomFilter());
            this.outWriteEnd.close();
        } 
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
