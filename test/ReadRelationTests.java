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
        String inputPath = GammaConstants.tablesPath + "client.txt";
        String correctPath = GammaConstants.correctOutputPath + "clientTuplesReadTest.txt";
        String testPath = GammaConstants.testOutputPath + "readClientTest.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();

        Connector read_print = new Connector("read_print");
        ReadRelation read = new ReadRelation(inputPath, read_print);
        Print print = new Print(read_print);
        ThreadList.run(print);
        Utility.validate(testPath, correctPath, false);
    }
    
    @Test
    public void ReadRelationViewingTest () throws Exception {
        String inputPath = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "viewingTuplesReadTest.txt";
        String testPath = GammaConstants.testOutputPath + "readViewingTest.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();

        Connector read_print = new Connector("read_print");
        ReadRelation read = new ReadRelation(inputPath, read_print);
        Print print = new Print(read_print);
        ThreadList.run(print);
        Utility.validate(testPath, correctPath, false);
    }    
}
