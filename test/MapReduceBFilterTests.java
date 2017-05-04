
import RegTest.Utility;
import basicConnector.*;
import gamma.*;
import gammaSupport.*;
import graphs.MapReduceBFilter;
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
public class MapReduceBFilterTests {
    
    @Test
    public void MapReduceBFilterTestFullMatch () throws Exception {
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBStream  = GammaConstants.tablesPath + "viewing.txt";
        // This should just be bFilterViewingFullMatchOut.txt but since this is
        // threaded they get printed in a different order
        String correctPath = GammaConstants.correctOutputPath + "mrBFilterViewingFullMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "mrBFilterViewingFullMatchOut.txt";
        
        Utility.redirectStdOut(testPath);

        ThreadList.init();
        
        Connector bloomInCon = new Connector("bloomInCon");
        ReadRelation readForBloom = new ReadRelation(inputPathForBloom, bloomInCon);
        Connector sinkAStreamCon = new Connector("sinkAStreamCon");
        Connector bitMapCon = new Connector("bitMapCon");
        Bloom b = new Bloom(bloomInCon, 0, sinkAStreamCon, bitMapCon);
        Sink sink_a = new Sink(sinkAStreamCon);

        Connector bStreamCon = new Connector("bStreamCon");
        ReadRelation readForStream = new ReadRelation(inputPathForBStream, bStreamCon);
        Connector bFilterBOut = new Connector("bFilterBOut");
        MapReduceBFilter bFilter = new MapReduceBFilter(bitMapCon, bStreamCon, 0, bFilterBOut);
        
        Print p = new Print(bFilterBOut);

        ThreadList.run(p);

        Utility.validate(testPath, correctPath, false);
    }
    
    @Test
    public void MapReduceBFilterTestPartialMatch () throws Exception {
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBStream  = GammaConstants.tablesPath + "bFilterViewingPartialMatch.txt";
        // This should just be bFilterViewingFullMatchOut.txt but since this is
        // threaded they get printed in a different order
        String correctPath = GammaConstants.correctOutputPath + "mrBFilterViewingPartialMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "mrBFilterViewingPartialMatchOut.txt";
        
        Utility.redirectStdOut(testPath);

        ThreadList.init();
        
        Connector bloomInCon = new Connector("bloomInCon");
        ReadRelation readForBloom = new ReadRelation(inputPathForBloom, bloomInCon);
        Connector sinkAStreamCon = new Connector("sinkAStreamCon");
        Connector bitMapCon = new Connector("bitMapCon");
        Bloom b = new Bloom(bloomInCon, 1, sinkAStreamCon, bitMapCon);
        Sink sink_a = new Sink(sinkAStreamCon);

        Connector bStreamCon = new Connector("bStreamCon");
        ReadRelation readForStream = new ReadRelation(inputPathForBStream, bStreamCon);
        Connector bFilterBOut = new Connector("bFilterBOut");
        MapReduceBFilter bFilter = new MapReduceBFilter(bitMapCon, bStreamCon, 1, bFilterBOut);
        
        Print p = new Print(bFilterBOut);

        ThreadList.run(p);

        Utility.validate(testPath, correctPath, false);
    }
    
    @Test
    public void MapReduceBFilterTestNoMatch () throws Exception {
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBStream  = GammaConstants.tablesPath + "bFilterViewingNoMatch.txt";
        // This should just be bFilterViewingFullMatchOut.txt but since this is
        // threaded they get printed in a different order
        String correctPath = GammaConstants.correctOutputPath + "mrBFilterViewingNoMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "mrBFilterViewingNoMatchOut.txt";
        
        Utility.redirectStdOut(testPath);

        ThreadList.init();
        
        Connector bloomInCon = new Connector("bloomInCon");
        ReadRelation readForBloom = new ReadRelation(inputPathForBloom, bloomInCon);
        Connector sinkAStreamCon = new Connector("sinkAStreamCon");
        Connector bitMapCon = new Connector("bitMapCon");
        Bloom b = new Bloom(bloomInCon, 1, sinkAStreamCon, bitMapCon);
        Sink sink_a = new Sink(sinkAStreamCon);

        Connector bStreamCon = new Connector("bStreamCon");
        ReadRelation readForStream = new ReadRelation(inputPathForBStream, bStreamCon);
        Connector bFilterBOut = new Connector("bFilterBOut");
        MapReduceBFilter bFilter = new MapReduceBFilter(bitMapCon, bStreamCon, 1, bFilterBOut);
        
        Print p = new Print(bFilterBOut);

        ThreadList.run(p);

        Utility.validate(testPath, correctPath, false);
    }
}
