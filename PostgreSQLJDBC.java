import java.sql.*;


public class PostgreSQLJDBC {
   public static void main(String args[]) {
      Connection c = null;
      try {
         Class.forName("com.vertica.jdbc.Driver");
         c = DriverManager
            .getConnection("jdbc:vertica://129.7.243.243:5433/cosc6340s17",
            "team01", "2DvtjSks");
         
      } catch (Exception e) {
         e.printStackTrace();
         System.err.println(e.getClass().getName()+": "+e.getMessage());
         System.exit(0);
      }
      System.out.println("Opened database successfully");


   }
}