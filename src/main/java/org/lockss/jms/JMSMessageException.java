package org.lockss.jms;

import javax.jms.JMSException;

public class JMSMessageException extends Exception {
  /**
   * Constructor that takes a message.
   * @param msg the detail message
   */
  public JMSMessageException(String msg) {
    super(msg);
  }

  /**
   * Constructor that takes a message and a root cause.
   * @param msg the detail message
   * @param cause the cause of the exception. This argument is generally
   * expected to be a proper subclass of {@link javax.jms.JMSException},
   * but can also be a JNDI NamingException or the like.
   */
  public JMSMessageException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor that takes a plain root cause, intended for
   * subclasses mirroring corresponding {@code javax.jms} exceptions.
   * @param cause the cause of the exception. This argument is generally
   * expected to be a proper subclass of {@link javax.jms.JMSException}.
   */
  public JMSMessageException(Throwable cause) {
    super(cause != null ? cause.getMessage() : null, cause);
  }


  /**
   * Convenience method to get the vendor specific error code if
   * the root cause was an instance of JMSException.
   * @return a string specifying the vendor-specific error code if the
   * root cause is an instance of JMSException, or {@code null}
   */
  public String getErrorCode() {
    Throwable cause = getCause();
    if (cause instanceof JMSException) {
      return ((JMSException) cause).getErrorCode();
    }
    return null;
  }

  /**
   * Return the detail message, including the message from the linked exception
   * if there is one.
   * @see javax.jms.JMSException#getLinkedException()
   */
  @Override
  public String getMessage() {
    String message = super.getMessage();
    Throwable cause = getCause();
    if (cause instanceof JMSException) {
      Exception linkedEx = ((JMSException) cause).getLinkedException();
      if (linkedEx != null) {
        String linkedMessage = linkedEx.getMessage();
        String causeMessage = cause.getMessage();
        if (linkedMessage != null && (causeMessage == null || !causeMessage.contains(linkedMessage))) {
          message = message + "; nested exception is " + linkedEx;
        }
      }
    }
    return message;
  }
}
