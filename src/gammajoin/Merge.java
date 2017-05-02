package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.WriteEnd;
import basicConnector.ReadEnd;
import gammaSupport.ThreadList;
import gammaSupport.Tuple;

public class Merge extends Thread {
	private ReadEnd[] insertReadArray;
	private WriteEnd writeOut;
	
	public Merge (Connector[] inputConnectorArray, Connector outputConnectorArray) {
		int i = 0;
		insertReadArray = new ReadEnd[inputConnectorArray.length];
        
        while(i < inputConnectorArray.length) {
        	insertReadArray[i] = inputConnectorArray[i].getReadEnd();
        	i++;
        }
        
        writeOut = outputConnectorArray.getWriteEnd();
        outputConnectorArray.setRelation(inputConnectorArray[0].getRelation());
        
        ThreadList.add(this);
	}
	
	public void run () {
		try {
            int i = 0;
            Tuple t;
            while(i<insertReadArray.length){
                t = runHelper(i);
                while(true){
                	writeOut.putNextTuple(t);
                    t = runHelper(i);
                    if(t == null)
                    	break;
                }
                i++;
            }
            
            writeOut.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(HSplit.class.getName()).log(Level.SEVERE, null, ex);
        }
	}
	
	private Tuple runHelper(int index) throws Exception {
		return insertReadArray[index].getNextTuple();
	}
}
