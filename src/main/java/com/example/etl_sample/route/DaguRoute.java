package com.example.etl_sample.route;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.sql.SqlComponent;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class DaguRoute extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        log.info("inside the route");
        // Configure the PostgreSQL JDBC driver

        getContext().getComponent("sql", SqlComponent.class)
            .setDataSource(createDataSource());

        boolean isServerUp = checkServerStatus("localhost", 5432);
        log.info("isServerUp:+" +isServerUp);


        // Define the SQL query to fetch data from PostgreSQL
        String sqlQuery =
            "select inst_to.rowguid    as institution_guid, "
                +"inst_to.name       as institution_name, "
                +"inst_from.rowguid  as from_institution_guid, "
                +"inst_from.name     as from_institution_name, "
                +"i.original_rowguid as invoice_original_guid, "
                +"rec.rowguid        as receive_guid, "
                +"recDet.rowguid     as receive_detail_guid, "
                +"iu_i.rowguid       as invoice_item_unit_guid, "
                +"itemunit.rowguid   as receive_item_unit_guid, "
                +"recDet.quantity, "
                +"recDet.batch       as batch_number, "
                +"recDet.expiry      as expiry_date "
                +"from receive.receive_detail recDet "
                +"left join receive.receive rec on recDet.receive_id = rec.id "
                +"left join receive.receive_status recSta on rec.receive_status_id = recSta.id "
                +"left join item.manufacturer manu on recDet.manufacturer_id = manu.id "
                +"left join item.country country on manu.country_id = country.id "
                +"left join common.loss_adj_reason laReas on recDet.loss_adj_reason_id = laReas.id "
                +"left join item.item_unit itemUnit on recDet.item_unit_id = itemUnit.id "
                +"left join item.item item on itemUnit.item_id = item.id "
                +"left join item.product prod on item.product_id = prod.id "
                +"left join item.program prog on item.program_id = prog.id "
                +"left join item.strength stre on item.strength_id = stre.id "
                +"left join item.dosage dos on item.dosage_id = dos.id "
                +"left join item.product_model prodMod on item.product_model_id = prodMod.id "
                +"left join item.product_series prodSer on item.product_series_id = prodSer.id "
                +"left join item.unit unit on itemUnit.unit_id = unit.id "
                +"left join item.ven ven on itemUnit.ven_id = ven.id "
                +"left join common.receive_type recTyp on rec.receive_type_id = recTyp.id "
                +"left join receive.invoice i on i.id = rec.invoice_id "
                +"left join receive.invoice_detail id on id.id = recDet.invoice_detail_id "
                +"left join institution.institution inst_from on inst_from.id = i.from_institution_id "
                +"left join institution.institution inst_to on inst_to.id = i.to_institution_id "
                +"left join item.item_unit iu_i on iu_i.id = id.item_unit_id "
                +"where recSta.code = 'SBMT' "
                +"limit 15; ";

//        String targetJdbcUrl = "jdbc:postgresql://localhost:5432/JsiReceive.receiveTable";
//        String targetUsername = "postgres";
//        String targetPassword = "123@#321";

        from("quartz://myTimer?cron=0 * * ? * *")
            .log("display time;")
            .to("direct:publish");
        from("direct:publish")
            .doTry()
            .to("sql:" + sqlQuery)
            // .log("before process: ${body}")
            // .process(exchange -> {
            //     // Remove the ID from the body
            //     List<Map<String, Object>> queryResult = exchange.getIn().getBody(List.class);

            //     if (queryResult != null && !queryResult.isEmpty()) {
            //         // Remove the "id" field from each row
            //         for (Map<String, Object> row : queryResult) {
            //             row.remove("id");
            //         }

            //         // Set the modified data as the body
            //         exchange.getIn().setBody(queryResult);
            //     } else {
            //         // Handle the case where the query result is empty or null
            //         exchange.getIn().setBody("No data found");
            //     }
            // })
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
            .setHeader("institution_guid", simple("${body['institution_guid']}"))
            .setHeader("institution_name", simple("${body['institution_name']}"))
            .setHeader("from_institution_guid", simple("${body['from_institution_guid']}"))
            .setHeader("from_institution_name", simple("${body['from_institution_name']}"))
            .setHeader("invoice_original_guid", simple("${body['invoice_original_guid']}"))
            .setHeader("receive_guid", simple("${body['receive_guid']}"))
            .setHeader("receive_detail_guid", simple("${body['receive_detail_guid']}"))
            .setHeader("invoice_item_unit_guid", simple("${body['invoice_item_unit_guid']}"))
            .setHeader("receive_item_unit_guid", simple("${body['receive_item_unit_guid']}"))
            .setHeader("quantity", simple("${body['quantity']}"))
            .setHeader("batch_number", simple("${body['batch_number']}"))
            .setHeader("expiry_date", simple("${body['expiry_date']}"))
            .process(exchange -> {
                String institution_guid = exchange.getIn().getHeader("institution_guid", String.class);
                String institution_name = exchange.getIn().getHeader("institution_name", String.class);
                String from_institution_guid = exchange.getIn().getHeader("from_institution_guid", String.class);
                String from_institution_name = exchange.getIn().getHeader("from_institution_name", String.class);
                String invoice_original_guid = exchange.getIn().getHeader("invoice_original_guid", String.class);
                String receive_guid = exchange.getIn().getHeader("receive_guid", String.class);
                String receive_detail_guid = exchange.getIn().getHeader("receive_detail_guid", String.class);
                String invoice_item_unit_guid = exchange.getIn().getHeader("invoice_item_unit_guid", String.class);
                String receive_item_unit_guid = exchange.getIn().getHeader("receive_item_unit_guid", String.class);
                String quantity = exchange.getIn().getHeader("quantity", String.class);
                String batch_number = exchange.getIn().getHeader("batch_number", String.class);
                Timestamp expiry_date = exchange.getIn().getHeader("expiry_date", Timestamp.class);

                try (Connection connection = dataReceive.getConnection()) {
                    // Check if data already exists
                    connection.setAutoCommit(false);
                    boolean dataExists = false;
                    try (PreparedStatement selectStatement = connection.prepareStatement(
                        "SELECT COUNT(*) FROM temp.my_table WHERE receive_detail_guid = cast(? as uuid)")) {
                        selectStatement.setString(1, receive_detail_guid);
                        try (ResultSet resultSet = selectStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int count = resultSet.getInt(1);
                                dataExists = (count > 0);
                            }
                        }
                    }

                    // Insert data if it doesn't exist

                    // Insert data if it doesn't exist
                    if (!dataExists) {
                        try (PreparedStatement insertStatement = connection.prepareStatement(
                            "INSERT INTO temp.my_table (institution_guid, institution_name, from_institution_guid, from_institution_name, " +
                                "invoice_original_guid, receive_guid, receive_detail_guid, invoice_item_unit_guid, " +
                                "receive_item_unit_guid, quantity, batch_number, expiry_date) " +
                                "VALUES (cast(? as uuid), ?, cast(? as uuid), ?, cast(? as uuid), " +
                                "cast(? as uuid), cast(? as uuid), cast(? as uuid), cast(? as uuid), ?, ?, ?)")) {
                            insertStatement.setObject(1, UUID.fromString(institution_guid));
                            insertStatement.setString(2, institution_name);
                            insertStatement.setObject(3, UUID.fromString(from_institution_guid));
                            insertStatement.setString(4, from_institution_name);
                            insertStatement.setObject(5, UUID.fromString(invoice_original_guid));
                            insertStatement.setObject(6, UUID.fromString(receive_guid));
                            insertStatement.setObject(7, UUID.fromString(receive_detail_guid));
                            insertStatement.setObject(8, UUID.fromString(invoice_item_unit_guid));
                            insertStatement.setObject(9, UUID.fromString(receive_item_unit_guid));
                            insertStatement.setString(10, quantity);
                            insertStatement.setString(11, batch_number);
                            insertStatement.setTimestamp(12, expiry_date);
                            insertStatement.executeUpdate();
                            connection.commit(); // Commit the transaction
                        }
                    }
                } catch (SQLException e) {
                    // Handle any exceptions
                    e.printStackTrace(); // Print the exception details for debugging purposes
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
        dataSource.setDatabaseName("postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("123@#321");

        return dataSource;
    }

    private DataSource createDataReceive() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("123@#321");
        // Set other PGSimpleDataSource configuration properties as needed
        return dataSource;
    }
}
