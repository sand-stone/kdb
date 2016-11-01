package kdb;

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import kdb.proto.XMessage.Message;
import kdb.rsm.ZabException;
import com.google.protobuf.InvalidProtocolBufferException;

public final class KdbRequestHandler extends HttpServlet {
  private static final Logger log = LogManager.getLogger(KdbRequestHandler.class);

  private final DataNode db;

  KdbRequestHandler(DataNode db) {
    this.db = db;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    // remove the leading slash from the request path and use that as the key.
    String key = request.getPathInfo();
    //log.info("Got GET request for {}", key);
    String value = "hello from kdb";
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentLength(value.length());
    response.getOutputStream().write(value.getBytes());
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    AsyncContext context = request.startAsync(request, response);
    // remove the leading slash from the request path and use that as the key.
    int length = request.getContentLength();
    if (length <= 0) {
      // Don't accept requests without content length.
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.setContentLength(0);
      context.complete();
      return;
    }
    byte[] value = new byte[length];
    request.getInputStream().read(value);
    Message msg = null;
    try {
      msg = Message.parseFrom(value);
      //log.info("msg input {}", msg);
      db.process(msg, context);
      return;
    } catch(InvalidProtocolBufferException e) {
      log.info("value count {}", value.length);
      e.printStackTrace();
      msg = MessageBuilder.buildErrorResponse("InvalidProtocolBufferException");
    } catch(ZabException.TooManyPendingRequests e) {
      //log.info(e);
      msg = MessageBuilder.busyMsg;
    } catch(ZabException.InvalidPhase e) {
      //log.info(e);
      msg = MessageBuilder.buildErrorResponse("InvalidPhase");
    } catch(KdbException e) {
      //log.info(e);
      msg = MessageBuilder.buildErrorResponse(e.getMessage());
    }
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    context.complete();
  }

}
