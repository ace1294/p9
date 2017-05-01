/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import RegTest.Utility;
import gammajoin.*;
import basicConnector.*;
import gammaSupport.*;
import org.junit.Test;

/**
 *
 * @author Jason
 */
public class ReadRelationTests {
    
    @Test
    public void clientTuplesTest() throws Exception {
        ThreadList.init();
        
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("client.txt", c);
        r.run();
        c.verifyRelation();
        ReadEnd readEnd = r.getReadEnd();
        Utility.redirectStdOut("out.txt");
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        Utility.validate("out.txt", "CorrectOutput/clientTuples.txt",true);
    }
    
    @Test
    public void viewingTuplesTest() throws Exception {
        ThreadList.init();
        
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("viewing.txt", c);
        r.run();
        c.verifyRelation();
        ReadEnd readEnd = r.getReadEnd();
        Utility.redirectStdOut("out.txt");
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        Utility.validate("out.txt", "CorrectOutput/viewingTuples.txt",true);
    }
    
    @Test
    public void ordersTuplesTest() throws Exception {
        ThreadList.init();
        
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("orders.txt", c);
        r.run();
        c.verifyRelation();
        ReadEnd readEnd = r.getReadEnd();
        Utility.redirectStdOut("out.txt");
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        Utility.validate("out.txt", "CorrectOutput/ordersTuples.txt",true);
    }
    
    @Test
    public void partsTuplesTest() throws Exception {
        ThreadList.init();
        
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("parts.txt", c);
        r.run();
        c.verifyRelation();
        ReadEnd readEnd = r.getReadEnd();
        Utility.redirectStdOut("out.txt");
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        Utility.validate("out.txt", "CorrectOutput/partsTuples.txt",true);
    }
    
    @Test
    public void odetailsTuplesTest() throws Exception {
        ThreadList.init();
        
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("odetails.txt", c);
        r.run();
        c.verifyRelation();
        ReadEnd readEnd = r.getReadEnd();
        Utility.redirectStdOut("out.txt");
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        System.out.println(readEnd.getNextString());
        Utility.validate("out.txt", "CorrectOutput/odetailsTuples.txt",true);
    }
}
