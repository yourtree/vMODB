package dk.ku.di.dms.vms.web_common.network;

import sun.misc.Unsafe;
import sun.nio.ch.IOStatus;
import sun.nio.ch.IOUtil;
import sun.nio.ch.Net;

import javax.xml.crypto.Data;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static java.util.concurrent.locks.LockSupport.park;

/**
 * While not a better option line (https://openjdk.org/jeps/373)
 * this implementation attempts to achieve higher performance for single-threaded
 * event loop.
 */
public class SingleThreadDatagramChannelImpl {

    private static final Unsafe unsafe = getUnsafe();

    // DatagramChannel baseChannel;

    private static Method send;
    private static Method beginWrite;
    private static Method receive;
    private static Method endRead;

    private static Object fd;

    private static Object sourceSockAddr;

    private static Class<?> clazz;

    public SingleThreadDatagramChannelImpl(){

    }

    private static DatagramChannel baseChannel;

    private static Object nativeDispatcher;

    static {

        try {
            clazz = Class.forName("sun.nio.ch.DatagramChannelImpl");

            Field ndField = clazz.getField( "nd" );
            ndField.setAccessible(true);
            nativeDispatcher = ndField.get( baseChannel );

            // sourceSockAddr = clazz

            fd = clazz.getField( "fd" );

            beginWrite = clazz.getMethod( "beginWrite" );

            send = clazz.getMethod( "sendFromNativeBuffer" );

            clazz.getMethod("endRead");

            receive = clazz.getMethod( "receive", ByteBuffer.class );

            receive = clazz.getMethod( "receive", ByteBuffer.class, Boolean.class );

            receive = clazz.getMethod( "receiveIntoNativeBuffer",
                    ByteBuffer.class,
                    Integer.class,
                    Integer.class,
                    Boolean.class);

            // I can handle the writing to a managed direct byte buffer directly
            // I don't need the java code to do that for me

            receive = clazz.getMethod("receive0",
                    FileDescriptor.class, Long.class, Long.class, Boolean.class);


        } catch (ClassNotFoundException |
                NoSuchFieldException |
                NoSuchMethodException |
                IllegalAccessException ignored) {

        }

    }

    private boolean okayToRetry(int n){
        return (n == IOStatus.UNAVAILABLE) || (n == IOStatus.INTERRUPTED);
    }

    public int send(ByteBuffer src, SocketAddress target){

        // send.invoke(fd, src, target);
        // implement end write  correctly.. get state and cleaner attributes
        // but we are not using connect, so this might not be necessary
        // https://www.ibm.com/docs/en/zos/2.4.0?topic=functions-connect
        return 1;
    }

    // single thread read... no need to resort to lock as found in the original impl
    // https://man7.org/linux/man-pages/man2/recv.2.html
    public SocketAddress receive(ByteBuffer bb) throws IOException, InvocationTargetException, IllegalAccessException {

        boolean blocking = this.baseChannel.isBlocking();
        for (;;) {
            int n = (int) receive.invoke(bb, false);
            if (blocking) {
                while (okayToRetry(n) && baseChannel.isOpen()) {
                    park(Net.POLLIN);
                    n = (int) receive.invoke(bb, false);
                }
            }
            // adjust bytebuffer position
        }

        // endRead... probably  like the end write

    }

    private static sun.misc.Unsafe getUnsafe() {
        Field unsafeField = null;
        try {
            unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}
