package gammajoin;

import java.util.logging.Level;
import java.util.logging.Logger;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import gammaSupport.Tuple;

public class Sink {
	private ReadEnd sObj;
	
	public Sink (Connector input) {
        sObj = input.getReadEnd();
    }
    
    public void run () {
        try {
            Tuple t = sObj.getNextTuple();
            while(true){
                t = sObj.getNextTuple();
                if(t == null)
                	break;
            }
        } catch (Exception ex) {
            Logger.getLogger(Sink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
