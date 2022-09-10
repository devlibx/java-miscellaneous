package io.github.devlibx.miscellaneous.flink.drools.generator;


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;

public class DroolFileGeneratorTest {

    @Test
    public void testDrfFileGenerator() throws FileNotFoundException {
        String runPath = new File(".").getAbsoluteFile().getPath();
        String finalFile = runPath + "/src/test/java/io/github/devlibx/miscellaneous/flink/drools/generator/SampleJavaFileFile.java";
        DroolFileGenerator fileGenerator = new DroolFileGenerator(finalFile);
        String out = fileGenerator.generateOut();
        System.out.println(out);
    }
}