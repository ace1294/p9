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
 */
public class Gamma extends ArrayConnectors {
    
    public Gamma (Connector aInStream, Connector bInStream, int joinKey1, int joinKey2, Connector out) throws Exception {       
        Connector[] splitToBloomConnectors = ArrayConnectors.initConnectorArray("splitToBloomConnectors");
        Connector[] splitToFilterConnectors = ArrayConnectors.initConnectorArray("splitToFilterConnectors");
        Connector[] bloomToFilterConnectors    = ArrayConnectors.initConnectorArray("bloomToFilterConnectors");
        Connector[] bloomToJoinConnectors = ArrayConnectors.initConnectorArray("splitToJoinConnectors");
        Connector[] filterToJoinConnectors    = ArrayConnectors.initConnectorArray("fileToJoinConnectors");
        Connector[] joinToMergeConnectors      = ArrayConnectors.initConnectorArray("splitToFilterConnectors");
        
        HSplit splitA = new HSplit(aInStream, splitToBloomConnectors, joinKey1);
        HSplit spltB = new HSplit(bInStream, splitToFilterConnectors, joinKey2);
        
        for (int i = 0; i < splitLen; i++) {
            Bloom bloom = new Bloom(splitToBloomConnectors[i], joinKey1, bloomToJoinConnectors[i], bloomToFilterConnectors[i]);
            BFilter bfilter = new BFilter(bloomToFilterConnectors[i], splitToFilterConnectors[i], joinKey2, filterToJoinConnectors[i]);
            HJoin hjoin = new HJoin(bloomToJoinConnectors[i], filterToJoinConnectors[i], joinKey1, joinKey2, joinToMergeConnectors[i]);
        }
        
        Merge merge = new Merge(joinToMergeConnectors, out);
    }
}
