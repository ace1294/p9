/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package graphs;

import basicConnector.*;
import gamma.*;
import gammaSupport.ArrayConnectors;

/**
 *
 * @author Jason
 * 
 * split both streams using same hash function
 * A and B tuples can join only if they have the same hash key
 * perform n joins rather than n2  in parallel
 * reconstitute join
 */
public class MapReduceHJoin extends ArrayConnectors {
    
    public MapReduceHJoin(Connector aInCon, Connector bInCon, int joinKey1, int joinKey2, Connector mergedOutCon) {
        Connector[] hSplitAToHJoinCons = ArrayConnectors.initConnectorArray("hsplit_a_hjoin");
        Connector[] hSplitBToHJoinCons = ArrayConnectors.initConnectorArray("hsplit_b_hjoin");
        Connector[] joinMergeCons    = ArrayConnectors.initConnectorArray("hjoin_merge");
        
        HSplit hsplit_a = new HSplit(aInCon, hSplitAToHJoinCons, joinKey1);
        HSplit hsplit_b = new HSplit(bInCon, hSplitBToHJoinCons, joinKey2);
        
        HJoin[] hjoins = new HJoin[ArrayConnectors.splitLen];
        for (int i = 0; i < ArrayConnectors.splitLen; i++) {
            hjoins[i] = new HJoin(hSplitAToHJoinCons[i], hSplitBToHJoinCons[i], joinKey1, joinKey2, joinMergeCons[i]);
        }
        
        Merge merge = new Merge(joinMergeCons, mergedOutCon);
    }
}
