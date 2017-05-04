
import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import graphs.MapReduceBloom;
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
public class MapReduceBloomTests {
    
    @Test
    public void MapReduceBloomTestAStream () throws Exception {
        String inputPath  = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "mrBloomViewingOutA.txt";
        String testPath = GammaConstants.testOutputPath + "mrBloomViewingOutA.txt";
        
        Utility.redirectStdOut(testPath);
        
        ThreadList.init();
        Connector inputConnector = new Connector("input");
        ReadRelation read = new ReadRelation(inputPath, inputConnector);
        Connector aStreamCon = new Connector("aStreamCon");
        Connector bitMapCon = new Connector("bitMapCon");
        MapReduceBloom b = new MapReduceBloom(inputConnector, 0, aStreamCon, bitMapCon);
        Print printAStream = new Print(aStreamCon);
        Sink sinkBitMap = new Sink(bitMapCon);
        
        ThreadList.run(printAStream);
        
        Utility.validate(testPath, correctPath, false);
    }
    
    @Test
    public void MapReduceBloomTestBitMap () throws Exception {
        String inputPath  = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "mrBloomViewingOutM.txt";
        String testPath = GammaConstants.testOutputPath + "mrBloomViewingOutM.txt";
        
        Utility.redirectStdOut(testPath);
        
        ThreadList.init();
        Connector inputConnector = new Connector("input");
        ReadRelation read = new ReadRelation(inputPath, inputConnector);
        Connector aStreamCon = new Connector("aStreamCon");
        Connector bitMapCon = new Connector("bitMapCon");
        MapReduceBloom b = new MapReduceBloom(inputConnector, 0, aStreamCon, bitMapCon);
        Sink sinkAStream = new Sink(aStreamCon);
        PrintMap printMap = new PrintMap(bitMapCon);
        
        ThreadList.run(printMap);
        
        Utility.validate(testPath, correctPath, false);
    }

}
