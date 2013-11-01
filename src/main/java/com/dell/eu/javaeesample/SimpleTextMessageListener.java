package com.dell.eu.javaeesample;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.logging.Logger;

public class SimpleTextMessageListener implements MessageListener {

    private final Logger logger = Logger.getLogger(getClass().getName());

    @Override
    public void onMessage(Message message) {
        if (TextMessage.class.isInstance(message)) {
            try {
                logger.info("Received message : " + ((TextMessage) message).getText());
            } catch (JMSException e) {
                logger.warning(e.getMessage());
            }
        }
    }
}
