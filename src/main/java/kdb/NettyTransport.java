package kdb;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.DecoderResult;
import io.netty.util.AsciiString;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import java.io.File;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import kdb.proto.XMessage.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import kdb.rsm.ZabException;
import java.util.List;
import java.util.ArrayList;

public class NettyTransport {
  private static Logger log = LogManager.getLogger(NettyTransport.class);

  public NettyTransport() { }

  static List<Ring> configRings(PropertiesConfiguration config, boolean standalone, Store store) {
    List ringaddrs = config.getList("ringaddr");
    List leaders = config.getList("leader");
    List logs = config.getList("logDir");

    int len = ringaddrs.size();
    if((leaders.size() > 0 && len != leaders.size()) || len != logs.size())
      throw new KdbException("ring config error");

    List<Ring> rings = new ArrayList<Ring>();
    for(int i = 0; i < len; i++) {
      Ring ring = new Ring((String)ringaddrs.get(i), leaders.size() == 0? null: (String)leaders.get(i), (String)logs.get(i));
      rings.add(ring);
      if(!standalone) {
        ring.bind(store);
      }
    }
    return rings;
  }

  public void start(PropertiesConfiguration config) {
    int port = config.getInt("port");
    boolean SSL = config.getBoolean("ssl", false);
    final SslContext sslCtx;
    if (SSL) {
      try {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
      } catch(Exception e) {
        throw new KdbException(e);
      }
    } else {
      sslCtx = null;
    }

    boolean standalone = config.getBoolean("standalone", false);
    Store store = new Store(config.getString("store"));
    DataNode datanode = new DataNode(configRings(config, standalone, store), store, standalone);

    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.option(ChannelOption.SO_BACKLOG, 1024);
      b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new HttpKdbServerInitializer(sslCtx, datanode));

      Channel ch = b.bind(port).sync().channel();
      ch.closeFuture().sync();
    } catch(Exception e) {
      throw new KdbException(e);
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  public static class HttpKdbServerHandler extends ChannelInboundHandlerAdapter {
    private static final byte[] Ping = { 'H', 'e', 'l', 'l', 'o', ' ', 'K', 'd', 'b'};

    private static final AsciiString CONTENT_TYPE = new AsciiString("Content-Type");
    private static final AsciiString CONTENT_LENGTH = new AsciiString("Content-Length");
    private static final AsciiString CONNECTION = new AsciiString("Connection");
    private static final AsciiString KEEP_ALIVE = new AsciiString("keep-alive");
    private DataNode datanode;

    public HttpKdbServerHandler(DataNode datanode) {
      this.datanode = datanode;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
      ctx.flush();
    }

    public static void reply(Object ctx, Message msg) {
      log.info("ctx {}", ctx);
      ChannelHandlerContext context = (ChannelHandlerContext)ctx;
      FullHttpResponse response;
      response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(msg.toByteArray()));
      response.headers().set(CONTENT_TYPE, "application/octet-stream");
      response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
      response.headers().set(CONNECTION, KEEP_ALIVE);
      context.write(response).addListener(ChannelFutureListener.CLOSE);;
      log.info("****");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) {
      if (data instanceof HttpRequest) {
        FullHttpResponse response;
        HttpRequest req = (HttpRequest)data;
        if (HttpUtil.is100ContinueExpected(req)) {
          ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
        }
        //log.info("req method {}", req.method());
        //log.info("req method {}", req.uri());
        if(req.getMethod() == HttpMethod.POST) {
          FullHttpMessage m = (FullHttpMessage) data;
          Message msg;
          ByteBuf buf = null;
          try {
            buf = m.content();
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            msg = Message.parseFrom(bytes);
            msg = datanode.process(msg, ctx);
          } catch(InvalidProtocolBufferException e) {
            //log.info(e);
            msg = MessageBuilder.buildErrorResponse("InvalidProtocolBufferException");
          } catch(KdbException e) {
            //log.info(e);
            msg = MessageBuilder.buildErrorResponse(e.getMessage());
          } finally {
            if(buf != null)
              buf.release();
          }
          response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(msg.toByteArray()));
          response.headers().set(CONTENT_TYPE, "application/octet-stream");
          response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
        } else {
          response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(Ping));
          response.headers().set(CONTENT_TYPE, "text/plain");
          response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
        }
        boolean keepAlive = HttpUtil.isKeepAlive(req);
        if (!keepAlive) {
          ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
          response.headers().set(CONNECTION, KEEP_ALIVE);
          ctx.write(response);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }

  public static class HttpKdbServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private DataNode datanode;

    public HttpKdbServerInitializer(SslContext sslCtx, DataNode datanode) {
      this.sslCtx = sslCtx;
      this.datanode = datanode;
    }

    @Override
    public void initChannel(SocketChannel ch) {
      ChannelPipeline p = ch.pipeline();
      if (sslCtx != null) {
        p.addLast(sslCtx.newHandler(ch.alloc()));
      }
      p.addLast(new HttpServerCodec());
      p.addLast("aggregator", new HttpObjectAggregator(165536));
      p.addLast(new HttpKdbServerHandler(datanode));
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/kdb-1.0-SNAPSHOT.jar kdb.HttpTransport conf/datanode.properties");
      return;
    }
    File propertiesFile = new File(args[0]);
    if(!propertiesFile.exists()) {
      System.out.printf("config file %s does not exist", propertiesFile.getName());
      return;
    }
    Configurations configs = new Configurations();
    PropertiesConfiguration config = configs.properties(propertiesFile);

    new NettyTransport().start(config);
  }
}
