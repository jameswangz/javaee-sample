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
    private static final String JMS_DESTINATION = "queue/test";
    private static final String JMS_MESSAGE = "Hello, JMS!";
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
        MessageProducer producer = null;
        MessageConsumer consumer = null;
        Destination destination = null;
        TextMessage message = null;
        Context context = null;

        try {
            context = new InitialContext();

            // Perform the JNDI lookups
            logger.info("Attempting to acquire connection factory \"" + JMS_CONNECTION_FACTORY + "\"");
            connectionFactory = (ConnectionFactory) context.lookup(JMS_CONNECTION_FACTORY);
            logger.info("Found connection factory \"" + JMS_CONNECTION_FACTORY + "\" in JNDI");

            String destinationString = System.getProperty("destination", JMS_DESTINATION);
            logger.info("Attempting to acquire destination \"" + destinationString + "\"");
            destination = (Destination) context.lookup(destinationString);
            logger.info("Found destination \"" + destinationString + "\" in JNDI");

            // Create the JMS connection, session, producer, and consumer
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(destination);
            consumer = session.createConsumer(destination);
            connection.start();

            logger.info("Sending " + JMS_MESSAGE_COUNT + " messages with content: " + JMS_MESSAGE);

            // Send the specified number of messages
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = session.createTextMessage(JMS_MESSAGE);
                producer.send(message);
            }

            // Then receive the same number of messages that were sent
            for (int i = 0; i < JMS_MESSAGE_COUNT; i++) {
                message = (TextMessage) consumer.receive(5000);
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
