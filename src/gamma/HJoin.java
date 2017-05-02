/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import basicConnector.*;
import gammaSupport.*;
import java.util.Hashtable;

/**
 *
 * @author Jason
 */
public class HJoin extends Thread {
    
    private final ReadEnd inReadEnd1;
    private final ReadEnd inReadEnd2;
    private final WriteEnd outWriteEnd;
    private final int joinKey1;
    private final int joinKey2;
    
    private final Relation relationValue;
    
    private final Hashtable<String, Tuple> table;
    
    public HJoin (Connector in1, Connector in2, int jKey1, int jKey2, Connector out) {
        
        this.inReadEnd1 = in1.getReadEnd();
        this.inReadEnd2 = in2.getReadEnd();
        this.joinKey1 = jKey1;
        this.joinKey2 = jKey2;
        this.outWriteEnd = out.getWriteEnd();
        
        this.relationValue = Relation.join(in1.getRelation(), in2.getRelation(), this.joinKey1, this.joinKey2);
        this.outWriteEnd.setRelation(relationValue);
        
        this.table = new Hashtable<String, Tuple>();
        
        ThreadList.add(this);
    }
    
    /*
    read all of stream A into a main memory hash table
    read B stream one tuple at a time; 
    hash join key of B’s tuple and join it to all A tuple’s with the same join key
    linear algorithm in that each A,B tuples is read only once
    */
    @Override
    public void run() {
        try {
            
            // Read all of input 1 into table
            Tuple t1 = this.inReadEnd1.getNextTuple();
            while(t1 != null){
                table.put(t1.get(this.joinKey1), t1);
                t1 = this.inReadEnd1.getNextTuple();
            }
            
            // Read all of input 2
            Tuple t2 = this.inReadEnd2.getNextTuple();
            while(t2 != null){
                t1 = table.get(t2.get(this.joinKey2));
                
                // If there's a match between join keys, output joined
                if(t1 != null){
                    Tuple t_out = Tuple.join(t1, t2, this.joinKey1, this.joinKey2);
                    this.outWriteEnd.putNextTuple(t_out);
                }
                t2  = this.inReadEnd2.getNextTuple();
            }
            this.outWriteEnd.close();
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        } 
    }
    
}
