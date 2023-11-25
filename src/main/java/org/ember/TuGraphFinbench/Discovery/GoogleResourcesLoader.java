package org.ember.TuGraphFinbench.Discovery;

import com.google.common.io.Resources;

import java.nio.charset.Charset;
import java.util.List;

public class GoogleResourcesLoader {
    public static void main(String[] args) {
        System.out.println("Try to open: `C:/Users/edwar/testResource.txt`");
        try {
            List<String> lines = Resources.readLines(Resources.getResource("C:/Users/edwar/testResource.txt"), Charset.defaultCharset());
            lines.forEach(System.out::println);
        } catch (Exception e) {
            System.out.println("Failed to open: `C:/Users/edwar/testResource.txt`, reason = " + e.toString());
        }
    }
}
