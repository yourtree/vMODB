package dk.ku.di.dms.vms.modb.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.LogManager;

public final class ConfigUtils {

    private static final String CONFIG_FILE = "app.properties";

    /*
     * Java does not automatically load logging.properties from the classpath by default.
     */
    static {
        try (InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream("logging.properties")) {
            if (inputStream != null) {
                LogManager.getLogManager().readConfiguration(inputStream);
            } else {
                System.err.println("Could not find logging.properties in the classpath");
            }
        } catch (Exception e) {
            System.out.println("Error loading logging configuration. Resorting to default configuration\n"+e.getMessage());
        }
    }

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
