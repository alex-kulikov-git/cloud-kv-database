package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ ConnectionTest.class, InteractionTest.class, AdditionalTest.class, EcsInteractionTest.class, ReplicationTest.class, PerformanceTest.class, ExtensionTest.class})
public class AllTests {

}
