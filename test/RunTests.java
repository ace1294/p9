/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 * @author nathanpoag
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({BFilterTests.class, SplitMTests.class, HJoinTests.class, HSplitTests.class, BloomSimulatorTests.class, ReadRelationTests.class, MergeTests.class, MergeMTests.class, BloomTests.class, MapReduceBFilterTests.class, MapReduceBloomTests.class, MapReduceHJoinTests.class, HJoinRefineWithBloomFiltersTests.class, GammaTests.class})
public class RunTests {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
}
