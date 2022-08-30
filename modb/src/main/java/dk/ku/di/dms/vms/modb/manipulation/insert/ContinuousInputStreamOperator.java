package dk.ku.di.dms.vms.modb.manipulation.insert;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface ContinuousInputStreamOperator extends Consumer<ByteBuffer> { }