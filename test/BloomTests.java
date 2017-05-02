
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
    public void BloomTestM () throws Exception {
        String in  = GammaConstants.tablesPath + "client.txt";
        String correct = GammaConstants.correctOutputPath + "bloomOutMap.txt";
        String test = GammaConstants.testOutputPath + "bloomOutMap.txt";
        
        Utility.redirectStdOut(test);
        
        ThreadList.init();
        Connector recordStream = new Connector("in1");
        ReadRelation r1 = new ReadRelation(in, recordStream);
        Connector aP = new Connector("aP");
        Connector mP = new Connector("mP");
        Bloom b = new Bloom(recordStream, aP, mP, 0);
        Sink sTups = new Sink(aP);
        PrintMap pMap = new PrintMap(mP);
        
        ThreadList.run(pMap);
        
        Utility.validate(test, correct,false);
    }

    @Test
    public void BloomTestA () throws Exception {
        String in  = GammaConstants.tablesPath + "client.txt";
        String correct = GammaConstants.correctOutputPath + "bloomOutStream.txt";
        String test = GammaConstants.testOutputPath + "bloomOutStream.txt";
        
        Utility.redirectStdOut(test);
        
        ThreadList.init();
        Connector recordStream = new Connector("in1");
        ReadRelation r1 = new ReadRelation(in, recordStream);
        Connector aP = new Connector("aP");
        Connector mP = new Connector("mP");
        Bloom b = new Bloom(recordStream, aP, mP, 0);
        Print pTups = new Print(aP);
        Sink sMap = new Sink(mP);
        
        ThreadList.run(pTups);

        Utility.validate(test, correct,false);
    }
}
