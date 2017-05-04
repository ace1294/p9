/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package graphs;

import basicConnector.*;
import gamma.*;
import gammaSupport.*;

/**
 *
 * @author Jason
 * 
 * HSPLIT stream A
 * compute Bloom filter on each substream
 * reconstitute stream A
 * form merge bit maps to produce single bit map M
 * 
 */
public class MapReduceBloom extends ArrayConnectors {
    
    private ReadEnd inReadEnd;
    private WriteEnd outWriteEnd;
    
    public MapReduceBloom (Connector aInCon, int joinKey, Connector aOutCon, Connector bitMapMCon) {
        Connector[] hSplitToBloom = ArrayConnectors.initConnectorArray("hSplitToBloom");
        Connector[] bloomAToMerge = ArrayConnectors.initConnectorArray("bloomAToMerge");
        Connector[] bloomMToMerge = ArrayConnectors.initConnectorArray("bloomMToMerge");
        
        HSplit hSplit = new HSplit(aInCon, hSplitToBloom, joinKey);
        Bloom[] bloom = new Bloom[ArrayConnectors.splitLen];
        for (int i = 0; i < ArrayConnectors.splitLen; i++) {
            bloom[i] = new Bloom(hSplitToBloom[i], joinKey, bloomAToMerge[i], bloomMToMerge[i]);
        }
        
        Merge merge = new Merge(bloomAToMerge, aOutCon);
        MergeM mergem = new MergeM(bloomMToMerge, bitMapMCon);
    }
}
