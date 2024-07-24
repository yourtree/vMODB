package dk.ku.di.dms.vms.modb.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static java.lang.System.Logger.Level.WARNING;

public final class ConfigUtils {

    private static final String CONFIG_FILE = "app.properties";

    public static Properties loadProperties(){
        Properties properties = new Properties();
        try {
            if (Files.exists(Paths.get(CONFIG_FILE))) {
                properties.load(new FileInputStream(CONFIG_FILE));
            } else {
                properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
            }
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getUserHome() {
        String userHome = System.getProperty("user.home");
        if(userHome == null){
            System.out.println("User home directory is not set in the environment. Resorting to /usr/local/lib");
            userHome = "/usr/local/lib";
        }
        return userHome;
    }

}
