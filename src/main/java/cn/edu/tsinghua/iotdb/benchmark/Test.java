package cn.edu.tsinghua.iotdb.benchmark;

import java.sql.*;
import java.util.Locale;

public class Test {
    public static void main(String[] args) throws SQLException {
        Connection conn;
        try {
            conn = DriverManager.getConnection(
                    String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/", "127.0.0.1", 5432), "crate", ""
            );
        } catch (SQLException e) {
            throw new SQLException("Cannot connect to the database", e);
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE TABLE executedFromJava (id int, name text);");
        }
    }
}
