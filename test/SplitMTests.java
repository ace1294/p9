
import gamma.PrintMap;
import gamma.Sink;
import gamma.SplitM;
import RegTest.Utility;
import basicConnector.*;
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
public class SplitMTests {
    
    public void SplitMTest(int i) throws Exception {
        String correct = GammaConstants.correctOutputPath + "splitMOut" + i + ".txt";
        String test = GammaConstants.testOutputPath + "splitMOut" + i + ".txt";
        
        Utility.redirectStdOut(test);
        ThreadList.init();

        /*
        ttttttt
        fffffff
        tftftft
        ttffttf
        */
        String bmap = "tttttttffffffftftftftttffttf";
        
        Connector in = new Connector("input");
        in.setRelation(Relation.dummy);
        in.getWriteEnd().putNextString(bmap);
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        SplitM hsplit = new SplitM(in, outs);
        
        Sink s0 = new Sink(outs[0]);
        Sink s1 = new Sink(outs[1]);
        Sink s2 = new Sink(outs[2]);
        Sink s3 = new Sink(outs[3]);
        
        PrintMap p = new PrintMap(outs[i]);
        ThreadList.run(p);

        Utility.validate(test, correct,false);
    }
    
    @Test
    public void SplitMTest0() throws Exception {
        SplitMTest(0);
        SplitMTest(1);
        SplitMTest(2);
        SplitMTest(3);
    }
    
    @Test
    public void SplitMTest1() throws Exception {
        SplitMTest(1);
    }
    
    @Test
    public void SplitMTest2() throws Exception {
        SplitMTest(2);
    }
    
    @Test
    public void SplitMTest3() throws Exception {
        SplitMTest(3);
    }

    
}
