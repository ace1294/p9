/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import graphs.Gamma;
import org.junit.Test;

/**
 *
 * @author Jason
 */
public class GammaTests {
    public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {      
        System.out.println( "Joining " + r1name + " with " + r2name );

        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1); 
        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Connector o = new Connector("output");
        Gamma gamma = new Gamma(c1, c2, jk1, jk2, o);
        Print p = new Print(o);
        ThreadList.run(p);
    }
    
    @Test
    public void joinPartsOdetailsTest() throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hJoinPartsOdetailsOut.txt";
        String testPath = GammaConstants.testOutputPath + "hJoinPartsOdetailsOut.txt";
        
        Utility.redirectStdOut(testPath);
        join(GammaConstants.tablesPath + "parts.txt", GammaConstants.tablesPath + "odetails.txt", 0, 1);
        Utility.validate(testPath, correctPath,true);
    }
    
    @Test
    public void joinClientViewingTest() throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hJoinClientViewingOut.txt";
        String testPath = GammaConstants.testOutputPath + "hJoinClientViewingOut.txt";
        
        Utility.redirectStdOut(testPath);       
        join(GammaConstants.tablesPath + "client.txt", GammaConstants.tablesPath + "viewing.txt", 0, 0);
        Utility.validate(testPath, correctPath,true);
    }
    
    @Test
    public void joinOrdersOdetailsTest() throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hJoinOrdersOdetailsOut.txt";
        String testPath = GammaConstants.testOutputPath + "hJoinOrdersOdetailsOut.txt";
        
        Utility.redirectStdOut(testPath);
        join(GammaConstants.tablesPath + "orders.txt", GammaConstants.tablesPath + "odetails.txt", 0, 0);
        Utility.validate(testPath, correctPath,true);
    }
}
