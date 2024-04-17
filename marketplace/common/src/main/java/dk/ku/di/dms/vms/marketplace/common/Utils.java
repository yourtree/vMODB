package dk.ku.di.dms.vms.marketplace.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public final class Utils {

    private static final String configFile = "app.properties";

    public static Properties loadProperties(){
        Properties properties = new Properties();
        try {

            if (Files.exists(Paths.get(configFile))) {
                System.out.println("Loading external config file: " + configFile);
                properties.load(new FileInputStream(configFile));
            } else {
                System.out.println("Loading internal config file: app.properties");
                properties.load(Utils.class.getClassLoader().getResourceAsStream(configFile));
            }

            return properties;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
