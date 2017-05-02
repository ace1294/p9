package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

public class HSplit extends Thread {
	private ReadEnd readEndArray;
    private WriteEnd[] writeOutEndArray;
    private int index;
    
    public HSplit (Connector inputArray, Connector[] outputArray, int joinKey) {
        
    	int i = 0;
    	readEndArray = inputArray.getReadEnd();
        
        writeOutEndArray = new WriteEnd[outputArray.length];
        while(i< outputArray.length){
        	writeOutEndArray[i] = outputArray[i].getWriteEnd();
            outputArray[i].setRelation(inputArray.getRelation());
            i++;
        }
        
        index = joinKey;
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            Tuple t = readEndArray.getNextTuple();
            while(true){
                int i = BMap.myhash(t.get(index));
                writeOutEndArray[i].putNextTuple(t);
                t = readEndArray.getNextTuple();
                if(t == null)
                	break;
            }
            int i = 0;
            while(i<writeOutEndArray.length){
            	writeOutEndArray[i].close();
                i++;
            }
        } catch (Exception ex) {
            Logger.getLogger(HSplit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
