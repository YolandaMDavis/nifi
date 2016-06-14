package org.apache.nifi.processors.standard.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

public class StandardUtils {

    public static ClassLoader getCustomClassLoader(String modulePath, ClassLoader parentClassLoader) throws MalformedURLException {
        String[] modules = modulePath != null? modulePath.split(",") : null;
        URL[] classpaths = getURLsForClasspath(modules);
        return createModuleClassLoader(classpaths,parentClassLoader);
    }

    protected static URL[] getURLsForClasspath(String[] modulePaths) throws  MalformedURLException {
        List<URL> additionalClasspath = new LinkedList<>();
        if (modulePaths != null) {
            for (String modulePathString : modulePaths) {
                File modulePath = new File(modulePathString);

                if (modulePath.exists()) {

                    additionalClasspath.add(modulePath.toURI().toURL());

                    if (modulePath.isDirectory()) {
                        File[] jarFiles = modulePath.listFiles(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return (name != null && name.endsWith(".jar"));
                            }
                        });

                        if (jarFiles != null) {

                            for (File jarFile : jarFiles) {
                                additionalClasspath.add(jarFile.toURI().toURL());
                            }
                        }
                    }
                } else {
                    throw new MalformedURLException("Path specified does not exist");
                }
            }
        }
        return additionalClasspath.toArray(new URL[additionalClasspath.size()]);
    }

    protected static ClassLoader createModuleClassLoader(URL[] modules,ClassLoader parentClassLoader) {
        return new URLClassLoader(modules, parentClassLoader);
    }

}
