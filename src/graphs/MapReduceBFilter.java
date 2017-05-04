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
 * split M into M1…Mn
 * hash split stream B into B1…Bn
 * filter Bi substreams in parallel
 * reconstitute stream B’
 * 
 */
public class MapReduceBFilter extends ArrayConnectors {
    
    public MapReduceBFilter (Connector mInCon, Connector bInCon, int joinKey, Connector bOutCon) throws Exception {
        Connector[] mSplitToFilter = ArrayConnectors.initConnectorArray("mSplitToFilter");
        Connector[] hSplitToFilter = ArrayConnectors.initConnectorArray("hSplitToFilter");
        Connector[] filterToMerge = ArrayConnectors.initConnectorArray("filterToMerge");
        
        SplitM splitm = new SplitM(mInCon, mSplitToFilter);
        HSplit hsplit = new HSplit(bInCon, hSplitToFilter, joinKey);
        
        BFilter[] bFilters = new BFilter[ArrayConnectors.splitLen];
        for(int i = 0; i < ArrayConnectors.splitLen; i++){
            bFilters[i] = new BFilter(mSplitToFilter[i], hSplitToFilter[i], joinKey, filterToMerge[i]);
        }
        
        Merge merge = new Merge(filterToMerge, bOutCon);
    }
}
