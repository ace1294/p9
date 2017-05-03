
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
public class MergeMTests {
    
    /*
    Takes 4 distinct bitmaps and merges them into a single bitmap,
    so we only really need one test
    */
    @Test
    public void MergeMTest () throws Exception {
        String correct = GammaConstants.correctOutputPath + "mergeMOut.txt";
        String test = GammaConstants.testOutputPath + "mergeMOut.txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();
        
        /*
        4 distinct bitmaps that will be merged into 1
        */
        Connector[] inputConnectors = new Connector[4];
        inputConnectors[0] = new Connector("input0");
        inputConnectors[0].setRelation(Relation.dummy);
        inputConnectors[1] = new Connector("input1");
        inputConnectors[1].setRelation(Relation.dummy);
        inputConnectors[2] = new Connector("input2");
        inputConnectors[2].setRelation(Relation.dummy);
        inputConnectors[3] = new Connector("input3");
        inputConnectors[3].setRelation(Relation.dummy);
        Connector out = new Connector("out");
        
        MergeM merge = new MergeM(inputConnectors, out);
        PrintMap p = new PrintMap(out);
        
        String bmap0 = "ffftffffffftffffffffffffffft";
        String bmap1 = "tfffftffffffffftffffffffffff";
        String bmap2 = "ftffffftffffffffffftffffffff";
        String bmap3 = "fftfffffftffffffffffffftffff";
        
        inputConnectors[0].getWriteEnd().putNextString(bmap0);
        inputConnectors[1].getWriteEnd().putNextString(bmap1);
        inputConnectors[2].getWriteEnd().putNextString(bmap2);
        inputConnectors[3].getWriteEnd().putNextString(bmap3);
        
        ThreadList.run(p);
        
        Utility.validate(test, correct,false);
    }
}
