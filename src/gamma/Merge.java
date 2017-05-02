package gamma;

import basicConnector.*;
import gammaSupport.*;

public class Merge extends Thread {
    
    private final ReadEnd[] inReadEnds;
    private final WriteEnd outWriteEnd;

    public Merge (Connector[] inputConnectorArray, Connector outputConnectorArray) {
        this.inReadEnds = new ReadEnd[inputConnectorArray.length];

        for (int i = 0; i < inputConnectorArray.length; i++) {
            this.inReadEnds[i] = inputConnectorArray[i].getReadEnd();
        }
        
        outputConnectorArray.setRelation(inputConnectorArray[0].getRelation());
        this.outWriteEnd = outputConnectorArray.getWriteEnd();
        
        ThreadList.add(this);
    }
    
    /*
    reads 4 streams in round-robin order to merge their results into a single output stream.
    */
    @Override
    public void run () {
        try {
            for (int i = 0; i < this.inReadEnds.length; i++) {
                ReadEnd curReadEnd = this.inReadEnds[i];
                Tuple t = curReadEnd.getNextTuple();
                while (t != null) {
                    this.outWriteEnd.putNextTuple(t);
                    t = curReadEnd.getNextTuple();
                }
            }

            this.outWriteEnd.close();
        } 
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
