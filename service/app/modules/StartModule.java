package modules;

import com.google.inject.AbstractModule;

/**
 * This class is responsible for creating instance of
 * ApplicationStart at server startup time.
 *
 * @author Jaikumar Soundara Rajan
 */
public class StartModule extends AbstractModule {
    @Override
    protected void configure() {
        System.out.println("StartModule:configure: Start");
        try {
            bind(ApplicationStart.class).asEagerSingleton();
        } catch (Exception | Error e) {
            e.printStackTrace();
            throw e;
        }
        System.out.println("StartModule:configure: End");

    }
}
