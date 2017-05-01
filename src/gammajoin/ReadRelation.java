/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;
import basicConnector.Connector;
import basicConnector.WriteEnd;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import java.util.StringTokenizer;
import gammaSupport.Tuple;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.logging.*;

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
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        
        
        
        this.outWriteEnd = con.getWriteEnd();
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
            }
            this.outWriteEnd.close();
        }catch (Exception e) {
            Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
}
