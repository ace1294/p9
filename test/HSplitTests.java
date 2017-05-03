
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
public class HSplitTests {
    
    /*
    HSplit reads in a record stream and hash-splits its contents across 4 streams,
    therefore we need 4 separate tests to test each of the 4 streams
    */
    
    public Connector[] setupSplitViewingTest () throws Exception {
        String inputPath  = GammaConstants.tablesPath + "viewing.txt";
        
        Connector inputConnector = new Connector("input");
        ReadRelation read = new ReadRelation(inputPath, inputConnector); 
        Connector[] outputConnectors = new Connector[4];
        outputConnectors[0] = new Connector("out0");
        outputConnectors[1] = new Connector("out1");
        outputConnectors[2] = new Connector("out2");
        outputConnectors[3] = new Connector("out3");
        
        HSplit hsplit = new HSplit(inputConnector, outputConnectors, 0);
        
        return outputConnectors;
    }
    
    @Test
    public void HSplitViewingTest0 () throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hSplitViewingOut0.txt";
        String testPath = GammaConstants.testOutputPath + "hSplitViewingOut0.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        
        Connector[] outputConnectors = setupSplitViewingTest();
        
        Print print0 = new Print(outputConnectors[0]);
        Sink sink1 = new Sink(outputConnectors[1]);
        Sink sink2 = new Sink(outputConnectors[2]);
        Sink sink3 = new Sink(outputConnectors[3]);
        
        ThreadList.run(print0);

        Utility.validate(testPath, correctPath, false);
    }

    @Test
    public void HSplitViewingTest1 () throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hSplitViewingOut1.txt";
        String testPath = GammaConstants.testOutputPath + "hSplitViewingOut1.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        
        Connector[] outputConnectors = setupSplitViewingTest();
        
        Sink sink0 = new Sink(outputConnectors[0]);
        Print print1 = new Print(outputConnectors[1]);
        Sink sink2 = new Sink(outputConnectors[2]);
        Sink sink3 = new Sink(outputConnectors[3]);
        
        ThreadList.run(print1);

        Utility.validate(testPath, correctPath, false);
    }

    @Test
    public void HSplitViewingTest2 () throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "hSplitViewingOut2.txt";
        String testPath = GammaConstants.testOutputPath + "hSplitViewingOut2.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        
        Connector[] outputConnectors = setupSplitViewingTest();
        
        Sink sink0 = new Sink(outputConnectors[0]);
        Sink sink1 = new Sink(outputConnectors[1]);
        Print print2 = new Print(outputConnectors[2]);
        Sink sink3 = new Sink(outputConnectors[3]);
        
        ThreadList.run(print2);

        Utility.validate(testPath, correctPath, false);
    }

    @Test
    public void HSplitViewingTest3 () throws Exception {        
        String correctPath = GammaConstants.correctOutputPath + "hSplitViewingOut3.txt";
        String testPath = GammaConstants.testOutputPath + "hSplitViewingOut3.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        
        Connector[] outputConnectors = setupSplitViewingTest();
        
        Sink sink0 = new Sink(outputConnectors[0]);
        Sink sink1 = new Sink(outputConnectors[1]);
        Sink sink2 = new Sink(outputConnectors[2]);
        Print print3 = new Print(outputConnectors[3]);
        
        ThreadList.run(print3);

        Utility.validate(testPath, correctPath, false);
    }
}
