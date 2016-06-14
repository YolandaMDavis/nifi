package org.apache.nifi.processors.standard.util;

import java.net.MalformedURLException;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStandardUtils {

    @Test
    public void testGetCustomClassLoader() throws MalformedURLException,ClassNotFoundException{
        final String jarFilePath = "src/test/resources/TestTransformFactory";
        ClassLoader customClassLoader =  StandardUtils.getCustomClassLoader(jarFilePath,this.getClass().getClassLoader());
        assertTrue(customClassLoader != null);
        assertTrue(customClassLoader.loadClass("TestCustomJoltTransform") != null);
    }

    @Test
    public void testGetCustomClassLoaderNoPathSpecified() throws MalformedURLException{
        final ClassLoader originalClassLoader = this.getClass().getClassLoader();
        ClassLoader customClassLoader =  StandardUtils.getCustomClassLoader(null,originalClassLoader);
        assertTrue(customClassLoader != null);
        try{
            customClassLoader.loadClass("TestCustomJoltTransform");
        }catch (ClassNotFoundException cex){
            assertTrue(cex.getLocalizedMessage().equals("TestCustomJoltTransform"));
            return;
        }
        fail("exception did not occur, class should not be found");
    }

    @Test
    public void testGetCustomClassLoaderWithInvalidPath() {
        final String jarFilePath = "src/test/resources/FakeTransformFactory/TestCustomJoltTransform.jar";
        try {
            StandardUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader());
        }catch(MalformedURLException mex){
            assertTrue(mex.getLocalizedMessage().equals("Path specified does not exist"));
            return;
        }
        fail("exception did not occur, path should not exist");
    }
}
