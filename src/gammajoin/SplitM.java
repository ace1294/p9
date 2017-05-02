package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ThreadList;

public class SplitM extends Thread {
	
	 private ReadEnd ReadIn;
	 private WriteEnd[] WriteOutArray;
	
	public SplitM (Connector inputsC, Connector[] outputsC) {
		int i = 0;
		ReadIn = inputsC.getReadEnd();
        
		WriteOutArray = new WriteEnd[outputsC.length];
		
		while(i < outputsC.length) {
        	WriteOutArray[i] = outputsC[i].getWriteEnd();
            outputsC[i].setRelation(Relation.dummy);
            i++;
        }
        
        ThreadList.add(this);
	}
	
	public void run () {
		try {
			int i = 0;
            String s = ReadIn.getNextString();
            while(i<WriteOutArray.length){
            	WriteOutArray[i].putNextString(BMap.mask(s, i));
            	WriteOutArray[i].close();
            	i++;
            }
        } 
        catch (Exception ex) {
            Logger.getLogger(SplitM.class.getName()).log(Level.SEVERE, null, ex);
        }
	}
}
