package dk.ku.di.dms.vms.modb.query.insert;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface ContinuousInputStreamOperator extends Consumer<ByteBuffer> { }