/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import common.reader.UniversalReader;
import java.io.IOException;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author kajo
 */
public class ReaderTest {
    private static UniversalReader ur;
    
    @BeforeClass
    public static void createReader() {
        ur = new UniversalReader();
    }
    
    
    @Test
    public void testAdminMessage() {
        byte ping = (byte) 28;
        // should test the reader
        
    }
    
    
    @Test
    public void testKVMessage() {
        
    }
}
