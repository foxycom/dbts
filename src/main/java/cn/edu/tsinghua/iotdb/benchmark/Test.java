package cn.edu.tsinghua.iotdb.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class Test {

    public static void main(String [] args) throws Exception
    {
        Class.forName("com.vertica.jdbc.Driver");
        Connection conn =
                DriverManager.getConnection("jdbc:vertica://localhost:5433/test",
                        "dbadmin", "");
        conn.setAutoCommit(false);

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE polygons(id INTEGER PRIMARY KEY, poly GEOGRAPHY)");

        int id = 0;
        int numBatches = 5;
        int rowsPerBatch = 10;

        //batch inserting WKT data
        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO polygons VALUES(?, ST_GeographyFromText(?))");
        for(int i = 0; i < numBatches; i++)
        {

            for(int j = 0; j < rowsPerBatch; j++)
            {
                //Insert your own WKT data here
                pstmt.setInt(1, id++);
                pstmt.setString(2, "POINT(13.433249763982083 48.588626678189186)");
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        conn.commit();
        pstmt.close();

        conn.close();
    }
}
