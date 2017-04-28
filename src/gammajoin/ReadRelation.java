/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;
import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import java.io.File;
import java.util.Scanner;
import java.util.StringTokenizer;
import gammaSupport.Tuple;

/**
 *
 * @author Jason
 */
public class ReadRelation extends Thread {
    
    WriteEnd out;
    ReadEnd in;
    Connector c;
    File f;
    
    public ReadRelation(String fileName, Connector con) {
        this.c = con;
        this.f = new File(fileName);
        this.out = con.getWriteEnd();
        
        //System.out.println(this);
        // It says in the project we need to do this but it looks like this crashes it?
        //ThreadList.add(this);
    }

    @Override
    public void run() {
        try {
            String entireFileContent = new Scanner(this.f).useDelimiter("\\Z").next();
            StringTokenizer stt = new StringTokenizer(entireFileContent,"\n");
            String relationString = stt.nextToken();
            StringTokenizer relationTokenizer = new StringTokenizer(relationString," ");
            Relation r = new Relation(relationString, relationTokenizer.countTokens() - 1);
            
            this.c.setRelation(r);
            
            // Skip this line
            stt.nextToken();
            while (stt.hasMoreTokens()){
                String token = stt.nextToken();
                Tuple t = Tuple.makeTupleFromFileData(r, token);
                
                this.out.putNextTuple(t);
                System.out.println(t.toString());
            }
        } catch (Exception e) {
            System.err.println(this.getClass().getName() + e);
        }
    }
    
    
}
