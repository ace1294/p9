/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import basicConnector.*;
import gammaSupport.*;

/**
 *
 * @author Jason
 */

public class PrintMap extends Thread {
    
    private ReadEnd inReadEndMap;

    public PrintMap(Connector connReadBMap) {
        this.inReadEndMap = connReadBMap.getReadEnd();

        ThreadList.add(this);
    }

    /*
    reads a single bit map and prints it.  Useful for debugging
    */
    @Override
    public void run() {
        try {
            String bitmapString = this.inReadEndMap.getNextString();
            for(int i = 0; i < GammaConstants.splitLen; i++) {
                for(int j = 0; j < GammaConstants.mapSize; j++) {
                    System.out.print(bitmapString.charAt(i * GammaConstants.mapSize + j));
                }
                System.out.println();
            }
        }
        catch(Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
}
