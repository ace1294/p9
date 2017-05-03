/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import org.junit.Test;

/**
 *
 * @author Jason
 */
public class MergeTests {
    
    @Test
    public void MergeTest () throws Exception {
        String correct = GammaConstants.correctOutputPath + "mergeOut.txt";
        String test = GammaConstants.testOutputPath + "mergeOut.txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();
        
        Connector[] inputConnecors = new Connector[4];
        inputConnecors[0] = new Connector("input0");
        inputConnecors[1] = new Connector("input1");
        inputConnecors[2] = new Connector("input2");
        inputConnecors[3] = new Connector("input3");
        // Took the tuples of viewing and split them up in 4 files randomly, first has 2
        // second has 1, third is blank, and fourth has the final 2. Should come out the same 
        // way the normal viewing test works
        ReadRelation read0 = new ReadRelation(GammaConstants.tablesPath + "viewingSplit0.txt", inputConnecors[0]); 
        ReadRelation read1 = new ReadRelation(GammaConstants.tablesPath + "viewingSplit1.txt", inputConnecors[1]); 
        ReadRelation read2 = new ReadRelation(GammaConstants.tablesPath + "viewingSplit2.txt", inputConnecors[2]); 
        ReadRelation read3 = new ReadRelation(GammaConstants.tablesPath + "viewingSplit3.txt", inputConnecors[3]); 
        Connector outputConnector = new Connector("output");
        
        Merge merge = new Merge(inputConnecors, outputConnector);
        Print print = new Print(outputConnector);
        ThreadList.run(print);
        
        Utility.validate(test, correct,false);
    }
}
