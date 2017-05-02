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
public class SplitM extends Thread{
    
    private final ReadEnd inReadEnd;
    private final WriteEnd[] outWriteEnds;
    
    public SplitM(Connector conIn, Connector[] conOuts) {
        this.inReadEnd = conIn.getReadEnd();
        this.outWriteEnds = new WriteEnd[conOuts.length];
        
        for(int i = 0; i< conOuts.length; ++i){
            this.outWriteEnds[i] = conOuts[i].getWriteEnd();
            conOuts[i].setRelation(Relation.dummy);
        }
        
        ThreadList.add(this);
    }
    
    /*
    splits a bitmap into 4 distinct bitmaps (inverse of MergeM)
    */
    @Override
    public void run () {
        try {
            String tupleString = this.inReadEnd.getNextString();
            for(int i = 0; i < this.outWriteEnds.length; i++){
                this.outWriteEnds[i].putNextString(BMap.mask(tupleString, i));
                this.outWriteEnds[i].close();
            }
        } 
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
