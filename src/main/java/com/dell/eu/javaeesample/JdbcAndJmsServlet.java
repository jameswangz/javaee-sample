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
            String messagesCountParam = req.getParameter("messagesCount");
            int messagesCount = messagesCountParam == null ? JMS_MESSAGE_COUNT : Integer.valueOf(messagesCountParam);
            jms(resp, getServletConfig().getInitParameter("jmsdestination"), messagesCount);
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

    private void jms(HttpServletResponse resp, String destinationName, int messagesCount) throws Exception {
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

            // Create the JMS connection, session, producer, and consumer
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = (Destination) context.lookup(destinationName);
            logger.info("Found destination " + destinationName + " in JNDI");
            MessageProducer producer = session.createProducer(destination);
            MessageConsumer consumer = session.createConsumer(destination);

            connection.start();

            logger.info("Sending  messages...");

            // Send the specified number of messages
            for (int i = 0; i < messagesCount; i++) {
                message = session.createTextMessage("Message to be sent to " + destinationName);
                producer.send(message);
            }

            resp.getWriter().println("Sent jms message within destination " + destinationName);
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
