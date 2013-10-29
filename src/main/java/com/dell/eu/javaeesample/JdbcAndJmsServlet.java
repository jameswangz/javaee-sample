package com.dell.eu.javaeesample;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class JdbcAndJmsServlet extends HttpServlet {

    private static final Logger logger = Logger.getLogger(JdbcAndJmsServlet.class.getName());

    // Set up all the default values

    private static final String JMS_CONNECTION_FACTORY = "RemoteConnectionFactory";
    private static final int JMS_MESSAGE_COUNT = 1;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            jdbc(resp);
            jms(resp);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void jdbc(HttpServletResponse resp) throws NamingException, SQLException, IOException {
        Context context = new InitialContext();
        DataSource dataSource = (DataSource) context.lookup("jboss/datasources/ExampleDS");
        String sql = "select name, value from information_schema.settings";

        try (java.sql.Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery();
        ) {
            while (resultSet.next()) {
                resultSet.getString("name");
                resultSet.getString("value");
            }
            logger.info("Executed sql " + sql);
            resp.getWriter().println("Executed SQL : " + sql);
        }
    }

    private void jms(HttpServletResponse resp) throws Exception {
        ConnectionFactory connectionFactory = null;
        Connection connection = null;
        Session session = null;
        TextMessage message = null;
        Context context = null;

        try {
            context = new InitialContext();

            // Perform the JNDI lookups
            connectionFactory = (ConnectionFactory) context.lookup(JMS_CONNECTION_FACTORY);
            logger.info("Found connection factory " + JMS_CONNECTION_FACTORY + " in JNDI");

            // Create the JMS connection, session, queueProducer, and queueConsumer
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = (Queue) context.lookup("queue/test");
            MessageProducer queueProducer = session.createProducer(queue);
            MessageConsumer queueConsumer = session.createConsumer(queue);

            Topic topic = (Topic) context.lookup("topic/test");
            MessageProducer topicProducer = session.createProducer(topic);
            MessageConsumer topicConsumer = session.createConsumer(topic);

            connection.start();

            logger.info("Sending  messages...");

            // Send the specified number of messages
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = session.createTextMessage("This is a queue message");
                queueProducer.send(message);
            }

            // Then receive the same number of messages that were sent
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = (TextMessage) queueConsumer.receive(5000);
                logger.info("Received message with content " + message.getText());
            }

            // Send the specified number of messages
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = session.createTextMessage("This is a topic message.");
                topicProducer.send(message);
            }

            // Then receive the same number of messages that were sent
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = (TextMessage) topicConsumer.receive(5000);
                logger.info("Received message with content " + message.getText());
            }

            resp.getWriter().println("Sent and received jms message.");
        } catch (Exception e) {
            logger.severe(e.getMessage());
            throw e;
        } finally {
            if (context != null) {
                context.close();
            }

            // closing the connection takes care of the session, producer, and consumer
            if (connection != null) {
                connection.close();
            }
        }
    }

}
