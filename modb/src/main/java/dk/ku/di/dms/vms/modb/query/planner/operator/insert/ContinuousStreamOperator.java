package dk.ku.di.dms.vms.modb.query.planner.operator.insert;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface ContinuousStreamOperator extends Consumer<ByteBuffer> {

    byte ZERO = 0;

}
