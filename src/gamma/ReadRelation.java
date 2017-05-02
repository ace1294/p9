/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma;

import basicConnector.*;
import gammaSupport.*;
import java.util.StringTokenizer;
import java.io.*;

/**
 *
 * @author Jason
 */
public class ReadRelation extends Thread {
    
    private BufferedReader inReader;
    private WriteEnd outWriteEnd;
    private Relation r;
    
    public ReadRelation(String fileName, Connector con) throws Exception {
        try {
            this.outWriteEnd = con.getWriteEnd();
            
            this.inReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            String relationString = this.inReader.readLine();
            StringTokenizer st = new StringTokenizer(relationString);
            this.r = new Relation(fileName, st.countTokens());
            
            while(st.hasMoreTokens()){
                r.addField(st.nextToken());
            }
            
            con.setRelation(r);
            
            // Dashed blank line
            this.inReader.readLine();
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
        
        ThreadList.add(this);
    }

    @Override
    public void run() {
        try {
            String input;
            Tuple t;
            // Parse rows into records
            while(true) {
                input = this.inReader.readLine();
                if(input == null || input.equals("")){
                    // No more data to consume
                    break;
                }
                t = Tuple.makeTupleFromFileData(r, input);
                this.outWriteEnd.putNextTuple(t);
//                System.out.println("gamma.ReadRelation.run()");
            }
            this.outWriteEnd.close();
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " " + e);
        }
    }
    
}
