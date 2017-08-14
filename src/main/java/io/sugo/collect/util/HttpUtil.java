package io.sugo.collect.util;

import io.sugo.collect.reader.file.DefaultFileReader;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fengxj on 7/20/17.
 */
public class HttpUtil {
  private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);
  public static String post(String url,String data){
    HttpClient client = new HttpClient();
    PostMethod postMethod = new PostMethod(url);
    try {
      postMethod.setRequestEntity(new StringRequestEntity(data, "text/plain", "UTF-8"));
      int statusCode = client.executeMethod(postMethod);

      if (statusCode != HttpStatus.SC_OK) {
        logger.warn("Method failed: " + postMethod.getStatusLine());
      }

      byte[] bytes = postMethod.getResponseBody();
      return new String(bytes);
    } catch (IOException  e) {
      logger.error("", e);
    } finally {
      // Release the connection.
      postMethod.releaseConnection();
    }
    return null;
  }

  public static boolean postTo(String url, String data) {
    boolean isSucceeded = false;

    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(url);
    try {
      method.addRequestHeader("Accept-Encoding","gzip");
      StringRequestEntity requestEntity = new StringRequestEntity(
              data,
              "application/json",
              "UTF-8"
      );
      method.setRequestEntity(requestEntity);
      int statusCode = client.executeMethod(method);
      if (logger.isDebugEnabled()) {
        byte[] responseBody = method.getResponseBody();
        logger.debug(new String(responseBody));
      }
      if (statusCode != HttpStatus.SC_OK) {
        logger.warn("Method failed: " + method.getStatusLine());
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("success to send to " + url);
        }
        isSucceeded = true;
      }
    } catch (HttpException e) {
      logger.error("Fatal protocol violation: ", e);
    } catch (IOException e) {
      logger.error("Fatal transport error: ", e);
    } finally {
      method.releaseConnection();
    }

    return isSucceeded;
  }

}
