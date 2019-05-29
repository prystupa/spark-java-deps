package com.prystupa;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.stream.IntStream;

public class Launcher {

    public static void main(String[] args) throws InterruptedException {

        IntStream.range(1, 5).boxed().forEach(index -> {
            SparkLauncher launcher = new SparkLauncher()
                    .setAppResource("build/libs/spark-java-deps-all-1.0-SNAPSHOT.jar")
                    .setMaster("spark://C02Y31VBJHD2:7077")
                    .setDeployMode("cluster")
                    .setAppName("Test Pi - " + index)
                    .setVerbose(false);

            try {
                launcher
                        .addAppArgs(index.toString())
                        .startApplication(new SparkAppHandle.Listener() {
                            @Override
                            public void stateChanged(SparkAppHandle handle) {
                                System.out.println("stateChanged " + index + ": " + handle.getState().toString());
                            }

                            @Override
                            public void infoChanged(SparkAppHandle handle) {
                                System.out.println("infoChanged " + index + ", appId: " + handle.getAppId());
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(1000 * 20);
    }
}
