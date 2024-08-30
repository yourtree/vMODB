package dk.ku.di.dms.vms.modb.common.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.LogManager;

public final class ConfigUtils {

    private static final String CONFIG_FILE = "app.properties";

    private static final String LOGGING_FILE = "logging.properties";

    private static final Properties PROPERTIES;

    static {
        // Java does not automatically load logging.properties from the classpath by default.
        try (InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream(LOGGING_FILE)) {
            if (inputStream != null) {
                LogManager.getLogManager().readConfiguration(inputStream);
            } else {
                System.out.println("Could not find logging file in the classpath. Resorting to default configuration.");
            }
        } catch (Exception e) {
            System.out.println("Error loading logging configuration. Resorting to default configuration. Error details:\n" + e.getMessage());
        }

        PROPERTIES = new Properties();
        try {
            PROPERTIES.load(ConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties loadProperties(){
        return PROPERTIES;
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
