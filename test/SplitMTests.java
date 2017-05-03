
import gamma.*;
import RegTest.Utility;
import basicConnector.*;
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
public class SplitMTests {
    
    /*
    Splits a bitmap into 4 distinct bitmaps so we need to test
    that each of the 4 bitmaps are correct
    */
    
    @Test
    public void SplitMTest0() throws Exception {
        SplitMTest(0);
    }
    
    @Test
    public void SplitMTest1() throws Exception {
        SplitMTest(1);
    }
    
    @Test
    public void SplitMTest2() throws Exception {
        SplitMTest(2);
    }
    
    @Test
    public void SplitMTest3() throws Exception {
        SplitMTest(3);
    }
    
    public void SplitMTest(int i) throws Exception {
        String correct = GammaConstants.correctOutputPath + "splitMOut" + i + ".txt";
        String test = GammaConstants.testOutputPath + "splitMOut" + i + ".txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();

        /*
        ttttttt
        fffffff
        tftftft
        ttffttf
        */
        String bmap = "tttttttffffffftftftftttffttf";
        
        Connector inputConnector = new Connector("input");
        inputConnector.setRelation(Relation.dummy);
        inputConnector.getWriteEnd().putNextString(bmap);
        Connector[] outputConnectors = new Connector[4];
        outputConnectors[0] = new Connector("output0");
        outputConnectors[1] = new Connector("output1");
        outputConnectors[2] = new Connector("output2");
        outputConnectors[3] = new Connector("output3");
        
        SplitM hsplit = new SplitM(inputConnector, outputConnectors);
        
        /*
        4 bitmaps being split to
        */
        Sink s0 = new Sink(outputConnectors[0]);
        Sink s1 = new Sink(outputConnectors[1]);
        Sink s2 = new Sink(outputConnectors[2]);
        Sink s3 = new Sink(outputConnectors[3]);
                
        PrintMap p = new PrintMap(outputConnectors[i]);
        ThreadList.run(p);

        Utility.validate(test, correct,false);
    }
    
}
