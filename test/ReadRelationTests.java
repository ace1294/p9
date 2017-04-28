/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import gammajoin.ReadRelation;
import basicConnector.Connector;
import org.junit.Test;

/**
 *
 * @author Jason
 */
public class ReadRelationTests {
    
    @Test
    public void initialTests() {
        Connector c = new Connector("test_connector");
        ReadRelation r = new ReadRelation("client.txt", c);
        r.start();
    }
}
