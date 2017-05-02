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
public class Sink extends Thread{
    
    private final ReadEnd inReadEndSink;
    
    public Sink (Connector inReadEnd) {
        this.inReadEndSink = inReadEnd.getReadEnd();
    }
    
    /*
    this box is useful in testing; it simply reads its inputs and does nothing with them.
    */
    @Override
    public void run () {
        try {
            Tuple tuple = this.inReadEndSink.getNextTuple();
            while(tuple != null){
                tuple = this.inReadEndSink.getNextTuple();
            }
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
