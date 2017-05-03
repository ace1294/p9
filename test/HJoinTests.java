
import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import org.junit.Test;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jason
 */
public class HJoinTests {
    
    public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {      
        System.out.println( "Joining " + r1name + " with " + r2name );

        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1); 
        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Connector o = new Connector("output");
        HJoin hj = new HJoin(c1, c2, jk1, jk2, o);
        Print p = new Print(o);
        ThreadList.run(p);
    }
    
    @Test
    public void joinPartsOdetailsTest() throws Exception {
        String correct = GammaConstants.correctOutputPath + "hJoinPartsOdetailsOut.txt";
        String test = GammaConstants.testOutputPath + "hJoinPartsOdetailsOut.txt";
        
        Utility.redirectStdOut(test);
        join(GammaConstants.tablesPath + "parts.txt", GammaConstants.tablesPath + "odetails.txt", 0, 1);
        Utility.validate(test, correct,true);
    }
    
    @Test
    public void joinClientViewingTest() throws Exception {
        String correct = GammaConstants.correctOutputPath + "hJoinClientViewingOut.txt";
        String test = GammaConstants.testOutputPath + "hJoinClientViewingOut.txt";
        
        Utility.redirectStdOut(test);       
        join(GammaConstants.tablesPath + "client.txt", GammaConstants.tablesPath + "viewing.txt", 0, 0);
        Utility.validate(test, correct,true);
    }
    
    @Test
    public void joinOrdersOdetailsTest() throws Exception {
        String correct = GammaConstants.correctOutputPath + "hJoinOrdersOdetailsOut.txt";
        String test = GammaConstants.testOutputPath + "hJoinOrdersOdetailsOut.txt";
        
        Utility.redirectStdOut(test);
        join(GammaConstants.tablesPath + "orders.txt", GammaConstants.tablesPath + "odetails.txt", 0, 0);
        Utility.validate(test, correct,true);
    }
}
