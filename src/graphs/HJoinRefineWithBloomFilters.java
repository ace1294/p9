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
public class HJoinRefineWithBloomFilters extends ArrayConnectors {
    
    public HJoinRefineWithBloomFilters(Connector aInStream, Connector bInStream, int joinKey1, int joinKey2, Connector out) throws Exception {       
        Connector bloomFilterConnector = new Connector("bloom_filter");
        Connector bloomJoinConnector = new Connector("bloomJoinConnector");
        Connector filterJoinConnector = new Connector("filterJoinConnector");
        
        Bloom bloom = new Bloom(aInStream, joinKey1, bloomJoinConnector, bloomFilterConnector);
        BFilter bfilter = new BFilter(bloomFilterConnector, bInStream, joinKey2, filterJoinConnector);
        HJoin hjoin = new HJoin(bloomJoinConnector, filterJoinConnector, joinKey1, joinKey2, out);
    }
}
