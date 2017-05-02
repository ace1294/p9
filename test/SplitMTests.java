
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
    
    @Test
    public void SplitMTest() throws Exception {
            Utility.redirectStdOut("out.txt");
        ThreadList.init();

        String bmap = "tttttttffffffftftftftttffttf";
        // ttttttt
        // fffffff
        // tftftft
        // ttffttf

        Connector in = new Connector("input");
        in.setRelation(Relation.dummy);
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
        
        in.getWriteEnd().putNextString(bmap);
        
        ThreadList.run(p);

        Utility.validate(outDir+"splitMOut"+i+".txt", correctDir+"splitMOut"+i+".txt",false);
    }
    
}
