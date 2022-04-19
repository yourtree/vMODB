package dk.ku.di.dms.vms.modb.common.utils;

import java.util.function.Consumer;

@FunctionalInterface
public interface Producer<T> {

    void subscribe(Consumer<T> consumer);

}
