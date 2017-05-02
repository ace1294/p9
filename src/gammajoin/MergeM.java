package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ThreadList;

public class MergeM extends Thread {
	private ReadEnd[] insertArray;
    private WriteEnd outputArray;
    private BMap mapStore;

    public MergeM (Connector[] cInputArray, Connector cOutputArray){
    	int i = 0;
    	insertArray = new ReadEnd[cInputArray.length];
        while(i<cInputArray.length){
        	insertArray[i] = cInputArray[i].getReadEnd();
        	i++;
        }
        
        outputArray = cOutputArray.getWriteEnd();
        cOutputArray.setRelation(Relation.dummy);
        
        mapStore = BMap.makeBMap();
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            int i = 0;
            String sValue = insertArray[i].getNextString();
            while(true){
                BMap tempStore = BMap.makeBMap(sValue);
                mapStore = BMap.or(mapStore, tempStore);
                sValue = insertArray[++i].getNextString();
                if(sValue == null && i > insertArray.length-1)
                	break;
            }
            BMap tempStore = BMap.makeBMap(sValue);
            mapStore = BMap.or(mapStore, tempStore);
            outputArray.putNextString(mapStore.getBloomFilter());
            outputArray.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(MergeM.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
