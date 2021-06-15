import java.sql.ResultSet;
import java.sql.SQLException;

public class Demo {

    static String sql = null;
    static main db1 = null;
    static ResultSet ret = null;

    public static void main(String[] args) {
        sql = "select * from emp";//SQL语句
        db1 = new main(sql);//创建DBHelper对象

        try {
            ret = db1.pst.executeQuery();//执行语句，得到结果集
            while (ret.next()) {
                int uid = ret.getInt(1);
                String ufname = ret.getString(2);
                String ulname = ret.getString(3);
                System.out.println(uid + "\t" + ufname + "\t" + ulname + "\t" );
            }//显示数据
            ret.close();
            db1.close();//关闭连接
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
