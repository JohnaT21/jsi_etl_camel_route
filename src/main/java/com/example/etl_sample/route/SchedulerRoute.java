package com.example.etl_sample.route;


import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.sql.SqlComponent;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Component
public class SchedulerRoute extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        log.info("inside the route");
        // Configure the PostgreSQL JDBC driver

        getContext().getComponent("sql", SqlComponent.class)
            .setDataSource(createDataSource());

        boolean isServerUp = checkServerStatus("localhost", 5432);
        log.info("isServerUp:+" +isServerUp);

        // Define the SQL query to fetch data from PostgreSQL
        String sqlQuery = "select * from testTable;";


//        String targetJdbcUrl = "jdbc:postgresql://localhost:5432/JsiReceive.receiveTable";
//        String targetUsername = "postgres";
//        String targetPassword = "123@#321";

        from("quartz://myTimer?cron=0/10 * * ? * * *")
            .log("display time;")
            .to("direct:publish");
        from("direct:publish")
            .doTry()
            .to("sql:" + sqlQuery)
            .log("before process: ${body}")
            .process(exchange -> {
                // Remove the ID from the body
                List<Map<String, Object>> queryResult = exchange.getIn().getBody(List.class);

                if (queryResult != null && !queryResult.isEmpty()) {
                    // Remove the "id" field from each row
                    for (Map<String, Object> row : queryResult) {
                        row.remove("id");
                    }

                    // Set the modified data as the body
                    exchange.getIn().setBody(queryResult);
                } else {
                    // Handle the case where the query result is empty or null
                    exchange.getIn().setBody("No data found");
                }
            })
            .log("${body}")
            .to("direct:processData")
            .doCatch(IOException.class)
            .log("Server is down. Retrying in 5 seconds...")
            .delay(5000) // Wait for 5 seconds before retrying
            .to("direct:publish") // Retry the route
            .end();


        DataSource dataReceive = createDataReceive();


        from("direct:processData")

            .log("receive data: ${body}")
            .split().body()
            .setHeader("name", simple("${body['name']}"))
            .setHeader("age", simple("${body['age']}"))
            .setHeader("email", simple("${body['email']}"))
            .process(exchange -> {
                String name = exchange.getIn().getHeader("name", String.class);
                int age = exchange.getIn().getHeader("age", Integer.class);
                String email = exchange.getIn().getHeader("email", String.class);

                try (Connection connection = dataReceive.getConnection()) {
                    // Check if data already exists
                    boolean dataExists = false;
                    try (PreparedStatement selectStatement = connection.prepareStatement(
                        "SELECT COUNT(*) FROM jsireceive.receivetable WHERE NAME = ? AND AGE = ? AND EMAIL = ?")) {
                        selectStatement.setString(1, name);
                        selectStatement.setInt(2, age);
                        selectStatement.setString(3, email);
                        try (ResultSet resultSet = selectStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int count = resultSet.getInt(1);
                                dataExists = (count > 0);
                            }
                        }
                    }

                    // Insert data if it doesn't exist
                    if (!dataExists) {
                        try (PreparedStatement insertStatement = connection.prepareStatement(
                            "INSERT INTO jsireceive.receivetable (NAME, AGE, EMAIL) VALUES (?, ?, ?)")) {
                            insertStatement.setString(1, name);
                            insertStatement.setInt(2, age);
                            insertStatement.setString(3, email);
                            insertStatement.executeUpdate();
                        }
                    }
                } catch (SQLException e) {
                    // Handle any exceptions
                }
            })
            .log("Inserted rows: ${header.insertedRows}");
    }



// ...

    public boolean checkServerStatus(String host, int port) {
        try (Socket socket = new Socket()) {
            InetSocketAddress serverAddress = new InetSocketAddress(host, port);
            socket.connect(serverAddress, 1000); // Timeout in milliseconds

            // Server is reachable
            return true;
        } catch (IOException e) {
            // Server is not reachable or an error occurred
            return false;
        }
    }
    private DataSource createDataSource() {
        // Create a PostgreSQL DataSource
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName("localhost");
        dataSource.setPortNumber(5432);
        dataSource.setDatabaseName("JsTest");
        dataSource.setUser("postgres");
        dataSource.setPassword("123@#321");

        return dataSource;
    }

    private DataSource createDataReceive() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:5432/JsTest");
        dataSource.setUser("postgres");
        dataSource.setPassword("123@#321");
        // Set other PGSimpleDataSource configuration properties as needed
        return dataSource;
    }
}

