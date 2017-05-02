package gammajoin;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import gammaSupport.GammaConstants;
import gammaSupport.ReportError;
import gammaSupport.ThreadList;

public class PrintMap extends Thread {
	private ReadEnd bitmapReader;

    public PrintMap(Connector bitmapConnectorReadRelation) {
        this.bitmapReader = bitmapConnectorReadRelation.getReadEnd();

        ThreadList.add(this);
    }

    public void run() {
        try {
            String bitmapStringStore = bitmapReader.getNextString();
            int i = 0;
            while(i < GammaConstants.splitLen) {
                int j = 0;
                while(j < GammaConstants.mapSize)
                {
                    System.out.print(bitmapStringStore.charAt(i * GammaConstants.mapSize + j));
                    j++;
                }
                System.out.println();
                i++;
            }
        }
        catch(Exception e) {
            ReportError.msg("PrintMap: " + this.getClass().getName() + e);
        }
    }
}
