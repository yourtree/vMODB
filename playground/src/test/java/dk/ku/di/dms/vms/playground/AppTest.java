package dk.ku.di.dms.vms.playground;

import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import org.junit.Test;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        ByteBuffer bb = MemoryManager.getTemporaryDirectBuffer();

        bb.putInt(10);
        bb.putFloat(1);

        long address = MemoryUtils.getByteBufferAddress(bb);

        jdk.internal.misc.Unsafe unsafe = MemoryUtils.UNSAFE;

        int valInt = unsafe.getInt( address );

        float valFloat = unsafe.getFloat( address + Integer.BYTES );

        assert 10 == valInt && 1 == valFloat;


    }
}
