
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
    public void BloomTestStream () throws Exception {
        String in  = GammaConstants.tablesPath + "client.txt";
        String correct = GammaConstants.correctOutputPath + "bloomOutStream.txt";
        String test = GammaConstants.testOutputPath + "bloomOutStream.txt";
        
        Utility.redirectStdOut(test);
        
        ThreadList.init();
        Connector recordStream = new Connector("in1");
        ReadRelation r1 = new ReadRelation(in, recordStream);
        Connector streamOutput = new Connector("streamOutput");
        Connector mapOutput = new Connector("mapOuput");
        Bloom b = new Bloom(recordStream, 0, streamOutput, mapOutput);
        Print pTups = new Print(streamOutput);
        Sink sMap = new Sink(mapOutput);
        
        ThreadList.run(pTups);

        Utility.validate(test, correct,false);
    }
    
    @Test
    public void BloomTestBitMap () throws Exception {
        String in  = GammaConstants.tablesPath + "client.txt";
        String correct = GammaConstants.correctOutputPath + "bloomOutMap.txt";
        String test = GammaConstants.testOutputPath + "bloomOutMap.txt";
        
        Utility.redirectStdOut(test);
        
        ThreadList.init();
        Connector recordStream = new Connector("in1");
        ReadRelation r1 = new ReadRelation(in, recordStream);
        Connector streamOutput = new Connector("streamOutput");
        Connector mapOutput = new Connector("mapOutput");
        Bloom b = new Bloom(recordStream, 0, streamOutput, mapOutput);
        Sink sTups = new Sink(streamOutput);
        PrintMap pMap = new PrintMap(mapOutput);
        
        ThreadList.run(pMap);
        
        Utility.validate(test, correct,false);
    }
}
