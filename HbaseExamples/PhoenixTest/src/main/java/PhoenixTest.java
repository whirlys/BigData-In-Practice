import java.sql.*;

/**
 * @program: HbaseExamples
 * @description: https://my.oschina.net/u/3511143/blog/1808787
 * @author: 赖键锋
 * @create: 2019-02-17 15:51
 **/
public class PhoenixTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection connection = DriverManager.getConnection("jdbc:phoenix:master:2181");
        PreparedStatement statement = connection.prepareStatement("select * from test");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("NAME"));
        }

        statement.close();
        connection.close();
    }
}
