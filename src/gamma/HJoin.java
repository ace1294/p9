/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import basicConnector.*;
import gammaSupport.*;
import java.io.File;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jason
 */
public class HJoin extends Thread {
    
    
    private final ReadEnd in1ReadEnd;
    private final ReadEnd in2ReadEnd;
    private final WriteEnd outWriteEnd;
    private final int joinKey1;
    private final int joinKey2;
    
    private Relation relationValue;
    
    private final Hashtable<String, Tuple> table;
    
    public HJoin(Connector in1, Connector in2, int jKey1, int jKey2, Connector out) {
        
        this.in1ReadEnd = in1.getReadEnd();
        this.in2ReadEnd = in2.getReadEnd();
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
            Tuple t1 = this.in1ReadEnd.getNextTuple();
            while(t1 != null){
                table.put(t1.get(this.joinKey1), t1);
                t1 = this.in1ReadEnd.getNextTuple();
            }
            
            // Read all of input 2
            Tuple t2 = this.in2ReadEnd.getNextTuple();
            while(t2 != null){
                t1 = table.get(t2.get(this.joinKey2));
                
                // If there's a match between join keys, output joined
                if(t1 != null){
                    Tuple t_out = Tuple.join(t1, t2, this.joinKey1, this.joinKey2);
                    this.outWriteEnd.putNextTuple(t_out);
                }
                t2  = this.in2ReadEnd.getNextTuple();
            }
            this.outWriteEnd.close();
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        } 
    }
    
}
