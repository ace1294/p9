package gamma;

import basicConnector.*;
import gammaSupport.*;

public class HSplit extends Thread {
    
    private ReadEnd inReadEnd;
    private WriteEnd[] outWriteEnds;
    private int joinKey;
    
    public HSplit (Connector inputArray, Connector[] outputArray, int joinKey) {
    	this.inReadEnd = inputArray.getReadEnd();
        
        this.outWriteEnds = new WriteEnd[outputArray.length];
        for (int i = 0; i < outputArray.length; i++) {
            this.outWriteEnds[i] = outputArray[i].getWriteEnd();
            outputArray[i].setRelation(inputArray.getRelation());
        }
        
        this.joinKey = joinKey;
        
        ThreadList.add(this);
    }
    
    /*
    reads in a record stream and hash-splits its contents across 4 streams 
    (numbered 0..3).  The icon suggests only 3 streams, but you should use 4.
    */
    @Override
    public void run () {
        try {
            Tuple t = this.inReadEnd.getNextTuple();
            while(t != null){
                int i = BMap.myhash(t.get(this.joinKey));
                this.outWriteEnds[i].putNextTuple(t);
                t = this.inReadEnd.getNextTuple();
            }
            
            for (WriteEnd outWriteEnd : this.outWriteEnds) {
                outWriteEnd.close();
            }
            
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
