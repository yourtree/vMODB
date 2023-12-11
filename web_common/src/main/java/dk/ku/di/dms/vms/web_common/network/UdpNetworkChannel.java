package dk.ku.di.dms.vms.web_common.network;

import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UdpNetworkChannel extends BaseNetworkChannel {

   private static final String DEFAULT_MULTICAST_ADDRESS = "228.5.6.7";
   private static final int DEFAULT_MULTICAST_PORT = 6789;

   private DatagramChannel datagramChannel;

   // non blocking queue
   private LinkedBlockingQueue<WriteContext> pendingWrites;

   private Map<Integer,InetSocketAddress> cachedInetSocketAddresses;

   private NetworkInterface ni;

   private InetSocketAddress multicastAddress;

   private Writer writer;

   private boolean THREAD_PER_TASK = false;

   /**
    * May include options, such as specific thread for writing
    * Otherwise it works in round-robin, one read and one write.
    *
    * Options:
    * - single thread mode OR one-writer one-reader
    * - multicast address
    * - recv and send buffer
    * - network interface
    */
   public UdpNetworkChannel(SocketAddress node, Map<Integer, SocketAddress> servers, INetworkListener listener) throws IOException {

      super(node, servers, listener);

      InetSocketAddress addr = null; // new InetSocketAddress(node.host, node.port);

      this.multicastAddress = new InetSocketAddress(DEFAULT_MULTICAST_ADDRESS, DEFAULT_MULTICAST_PORT);

      this.ni = NetworkInterface.getByInetAddress(addr.getAddress());

      this.datagramChannel = DatagramChannel
              .open(StandardProtocolFamily.INET)
              .setOption(StandardSocketOptions.SO_REUSEADDR, true)
              .bind(addr)
              .setOption(StandardSocketOptions.IP_MULTICAST_IF, ni);

      //
      this.datagramChannel.configureBlocking(false);

      //
      this.writer = new Writer();
      this.writer.run();

      // move to main
      System.setSecurityManager(null);

   }

   @Override
   public void run() {

      if(THREAD_PER_TASK){

         // allocate direct a big portion of memory, create various slices
         ByteBuffer bb = ByteBuffer.allocate(16834);
         Queue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<>();

         while(isRunning()) {

            try {

               SocketAddress res = datagramChannel.receive(bb);
               // the election worker, on implementing on message
               // will read the buffer, create own message and finish

               if(res == null){ }

               // TODO test this code. does this thread blocks until accept finishes?
               // which thread is executing the accept?
               // if it is this thread, does it block until accept finishes?
               var cf = listener.onMessage(bb);
               cf.thenAccept(f -> {
                  bb.clear();
                  bufferQueue.add(bb);
                  // return null;
               });

            } catch (IOException ignored) {}
         }

      } else {

         ByteBuffer bb = ByteBuffer.allocate(16834);
         Queue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<>();
         try {
            while (isRunning()) {

               // round-robin read and write approach

               // read turn
               SocketAddress res = datagramChannel.receive(bb);

               if (res != null) { }

               // write turn



            }

         } catch (IOException ignored) {}

      }

   }

   private record WriteContext(
           SocketAddress node, ByteBuffer message
   ){}

   private void processWrite(){

      WriteContext write = pendingWrites.peek();
      InetSocketAddress addr = cachedInetSocketAddresses.get(write.node);

      // only send
      if(addr != null){

         try {
            datagramChannel.send( write.message, addr );
         } catch (IOException ignored) { }

      } else {
         // cachedInetSocketAddresses.put( write.node.hashCode(), InetSocketAddress() )
      }

   }

   /**
    * A stoppable thread to process writes
    */
   private class Writer extends SignalingStoppableRunnable {
      @Override
      public void run() {
         while(isRunning()) processWrite();
      }
   }

   @Override
   public void send(SocketAddress node, ByteBuffer message) {
      pendingWrites.add(new WriteContext(node,message));
   }

   /**
    * If default group, null as parameter
    * https://stackoverflow.com/questions/41857345/java-multicast-socket-not-receiving-on-specific-network-interface
    */
   @Override
   public void join() {

      try {

//         InetAddress multicastAddress;
//         if(group == null){
//            multicastAddress = InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS);
//         } else {
//            multicastAddress = InetAddress.getByName( group.host );
//         }

         // do we need this key?
         // MembershipKey key =

                 datagramChannel.join(multicastAddress.getAddress(), ni);

      } catch (IOException ignored) { }

   }

   @Override
   public void multicast(ByteBuffer message) {
      try {
         datagramChannel.send( message, multicastAddress );
      } catch (IOException ignored) { }
   }

   @Override
   public void close() {
      try {
         this.writer.stop();
         this.datagramChannel.close();
      } catch (IOException ignored) { }
   }


}
