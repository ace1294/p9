
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
public class BloomTests {
    
    @Test
    public void BloomViewingTestStream () throws Exception {
        String inputPath  = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "bloomViewingOutStream.txt";
        String testPath = GammaConstants.testOutputPath + "bloomViewingOutStream.txt";
        
        Utility.redirectStdOut(testPath);
        
        ThreadList.init();
        Connector recordStream = new Connector("input");
        ReadRelation r1 = new ReadRelation(inputPath, recordStream);
        Connector streamOutput = new Connector("streamOutput");
        Connector mapOutput = new Connector("mapOuput");
        Bloom b = new Bloom(recordStream, 0, streamOutput, mapOutput);
        Print pTups = new Print(streamOutput);
        Sink sMap = new Sink(mapOutput);
        
        ThreadList.run(pTups);

        Utility.validate(testPath, correctPath, false);
    }
    
    @Test
    public void BloomViewingTestBitMap () throws Exception {
        String inputPath  = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "bloomViewingOutMap.txt";
        String testPath = GammaConstants.testOutputPath + "bloomViewingOutMap.txt";
        
        Utility.redirectStdOut(testPath);
        
        ThreadList.init();
        Connector recordStream = new Connector("input");
        ReadRelation r1 = new ReadRelation(inputPath, recordStream);
        Connector streamOutput = new Connector("streamOutput");
        Connector mapOutput = new Connector("mapOutput");
        Bloom b = new Bloom(recordStream, 0, streamOutput, mapOutput);
        Sink sTups = new Sink(streamOutput);
        PrintMap pMap = new PrintMap(mapOutput);
        
        ThreadList.run(pMap);
        
        Utility.validate(testPath, correctPath, false);
    }
}
