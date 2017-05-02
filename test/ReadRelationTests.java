/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import gamma.ReadRelation;
import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import org.junit.Test;

/**
 *
 * @author Jason
 */
public class ReadRelationTests {
    
    @Test
    public void ReadRelationClientTest () throws Exception{
        String in = GammaConstants.tablesPath + "client.txt";
        String correct = GammaConstants.correctOutputPath + "clientTuplesReadTest.txt";
        String test = GammaConstants.testOutputPath + "readClientTest.txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();

        Connector read_print = new Connector("read_print");
        ReadRelation read = new ReadRelation(in, read_print);
        Print print = new Print(read_print);
        ThreadList.run(print);
        Utility.validate(test, correct, false);
    }
    
    @Test
    public void ReadRelationViewingTest () throws Exception {
        String in = GammaConstants.tablesPath + "viewing.txt";
        String correct = GammaConstants.correctOutputPath + "viewingTuplesReadTest.txt";
        String test = GammaConstants.testOutputPath + "readViewingTest.txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();

        Connector read_print = new Connector("read_print");
        ReadRelation read = new ReadRelation(in, read_print);
        Print print = new Print(read_print);
        ThreadList.run(print);
        Utility.validate(test, correct, false);
    }    
}
