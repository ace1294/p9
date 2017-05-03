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
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(pathForBloom, c1);
        Connector a_sink = new Connector("a_sink");
        Connector m_filter = new Connector("m_filter");
        Bloom b = new Bloom(c1, 1, a_sink, m_filter);
        Sink sink_a = new Sink(a_sink);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(pathForFilter, c2);
        Connector b_out = new Connector("b_out");
        BFilter bfilter = new BFilter(m_filter, c2, 1, b_out);
        
        Print p = new Print(b_out);
        
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
