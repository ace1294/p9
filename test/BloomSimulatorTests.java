/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author nathanpoag
 */
import RegTest.Utility;
import basicConnector.Connector;
import gammaSupport.GammaConstants;
import gammaSupport.ThreadList;
import gamma.*;
import org.junit.Test;


public class BloomSimulatorTests {
    @Test
    public void BloomSimulatorTestBitmap () throws Exception {
        String correctPath = GammaConstants.correctOutputPath + "bloomSimulatorOutMap.txt";
        String testPath = GammaConstants.correctOutputPath + "bloomSimulatorOutMap.txt";
        
        Utility.redirectStdOut(testPath);
        ThreadList.init();
        Connector mapOutput = new Connector("mapOutput");
        BloomSimulator b = new BloomSimulator(mapOutput);
        PrintMap printMap = new PrintMap(mapOutput);
        
        ThreadList.run(printMap);
        
        Utility.validate(testPath, correctPath, false);
    }
}
