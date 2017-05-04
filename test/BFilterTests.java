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
public class BFilterTests {
    
    public Print setupFilterViewingTest(String pathForBloom, String pathForFilter) throws Exception {
        Connector bloomInputCon = new Connector("bloomInputCon");
        ReadRelation readForBloom = new ReadRelation(pathForBloom, bloomInputCon);
        Connector sinkBloomTupleStream = new Connector("sinkBloomTupleStream");
        Connector bitMapConnector = new Connector("bitMapConnector");
        Bloom bloom = new Bloom(bloomInputCon, 1, sinkBloomTupleStream, bitMapConnector);
        Sink sinkAStream = new Sink(sinkBloomTupleStream);

        Connector streamInputCon = new Connector("filterInputCon");
        ReadRelation readFilter = new ReadRelation(pathForFilter, streamInputCon);
        Connector bStreamOut = new Connector("bStreamOut");
        BFilter bfilter = new BFilter(bitMapConnector, streamInputCon, 1, bStreamOut);
        
        Print p = new Print(bStreamOut);
        
        return p;
    }
    
    @Test
    public void BFilterTestFullMatch () throws Exception{
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBFilter  = GammaConstants.tablesPath + "viewing.txt";
        String correctPath = GammaConstants.correctOutputPath + "bFilterViewingFullMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "bFilterViewingFullMatchOut.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        Print p = setupFilterViewingTest(inputPathForBloom, inputPathForBFilter);
        ThreadList.run(p);
        Utility.validate(testPath, correctPath, false);
    }

    @Test
    public void BFilterTestPartialMatch () throws Exception{
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBFilter  = GammaConstants.tablesPath + "bFilterViewingPartialMatch.txt";
        String correctPath = GammaConstants.correctOutputPath + "bFilterViewingPartialMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "bFilterViewingPartialMatchOut.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        Print p = setupFilterViewingTest(inputPathForBloom, inputPathForBFilter);
        ThreadList.run(p);
        Utility.validate(testPath, correctPath, false);
    }

    @Test
    public void BFilterTestNoMatch () throws Exception{
        String inputPathForBloom  = GammaConstants.tablesPath + "viewing.txt";
        String inputPathForBFilter  = GammaConstants.tablesPath + "bFilterTestViewingNoMatch.txt";
        String correctPath = GammaConstants.correctOutputPath + "bFilterViewingNoMatchOut.txt";
        String testPath = GammaConstants.testOutputPath + "bFilterViewingNoMatchOut.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        Print p = setupFilterViewingTest(inputPathForBloom, inputPathForBFilter);
        ThreadList.run(p);
        Utility.validate(testPath, correctPath, false);
    }
}
