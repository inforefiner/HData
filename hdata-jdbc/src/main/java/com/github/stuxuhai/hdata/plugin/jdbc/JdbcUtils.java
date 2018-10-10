package com.github.stuxuhai.hdata.plugin.jdbc;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcUtils {

    /**
     * 获取表的字段类型
     *
     * @param connection
     * @param table
     * @return
     * @throws SQLException
     */
    public static Map<String, Integer> getColumnTypes(Connection connection, String table, String keywordEscaper) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(keywordEscaper);
        sql.append(table);
        sql.append(keywordEscaper);
        sql.append(" WHERE 1=2");

        ResultSetHandler<Map<String, Integer>> handler = new ResultSetHandler<Map<String, Integer>>() {
            @Override
            public Map<String, Integer> handle(ResultSet rs) throws SQLException {
                Map<String, Integer> map = new HashMap<String, Integer>();
                ResultSetMetaData rsd = rs.getMetaData();
                for (int i = 0; i < rsd.getColumnCount(); i++) {
                    map.put(rsd.getColumnName(i + 1).toLowerCase(), rsd.getColumnType(i + 1));
                }
                return map;
            }
        };

        QueryRunner runner = new QueryRunner();
        return runner.query(connection, sql.toString(), handler);
    }

    /**
     * 获取表的字段名称
     *
     * @param conn
     * @param table
     * @return
     * @throws SQLException
     */
    public static List<String> getColumnNames(Connection conn, String table, String keywordEscaper) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(keywordEscaper);
        sql.append(table);
        sql.append(keywordEscaper);
        sql.append(" WHERE 1=2");
        sql.append(" Limit 1");

        ResultSetHandler<List<String>> handler = new ResultSetHandler<List<String>>() {

            @Override
            public List<String> handle(ResultSet rs) throws SQLException {
                List<String> columnNames = new ArrayList<String>();
                ResultSetMetaData rsd = rs.getMetaData();

                for (int i = 0, len = rsd.getColumnCount(); i < len; i++) {
                    columnNames.add(rsd.getColumnName(i + 1));
                }
                return columnNames;
            }
        };

        QueryRunner runner = new QueryRunner();
        return runner.query(conn, sql.toString(), handler);
    }


    public static String getMaxValue(Connection conn, final String sql, final String column) throws SQLException {
        Pattern p = Pattern.compile("\\s+FROM\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (m.find() && column != null && !column.trim().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT MAX(");
            sb.append(column);
            sb.append(")");
            sb.append(m.group(0));
            ResultSetHandler<String> handler = new ResultSetHandler<String>() {
                @Override
                public String handle(ResultSet rs) throws SQLException {
                    String val = null;
                    while (rs.next()) {
                        Object obj = rs.getObject(1);
                        if (obj != null) {
                            val = obj.toString();
                        }
                    }
                    return val;
                }
            };
            QueryRunner runner = new QueryRunner();
            return runner.query(conn, sb.toString(), handler);
        }
        return "";
    }


    public static boolean isOracle(String driver) {
        return driver.indexOf("oracle") > -1 ? true : false;
    }

    public static boolean isDB2(String driver) {
        return driver.indexOf("db2") > -1 ? true : false;
    }

    public static boolean isPG(String driver) {
        return driver.indexOf("postgresql") > -1 ? true : false;
    }

    public static boolean isSqlServer(String driver) {
        return driver.indexOf("sqlserver") > -1 ? true : false;
    }

    public static long getCount(Connection conn, final String sql) throws SQLException {
        long count = 0l;
        Pattern p = Pattern.compile("\\s+FROM\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        if (m.find()) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT COUNT(*) ");
            sb.append(m.group(0));
            ResultSetHandler<Long> handler = new ResultSetHandler<Long>() {
                @Override
                public Long handle(ResultSet rs) throws SQLException {
                    long count = 0l;
                    while (rs.next()) {
                        count = rs.getLong(1);
                    }
                    return count;
                }
            };
            QueryRunner runner = new QueryRunner();
            return runner.query(conn, sb.toString(), handler);
        }
        return count;
    }

    /**
     * 查询表中分割字段值的区域（最大值、最小值）
     *
     * @param conn
     * @param sql
     * @param splitColumn
     * @return
     * @throws SQLException
     */
    public static long[] querySplitColumnRange(Connection conn, final String sql, final String splitColumn) throws SQLException {
        long[] minAndMax = new long[3];
        Pattern p = Pattern.compile("\\s+FROM\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);

        if (m.find() && splitColumn != null && !splitColumn.trim().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT MIN(");
            sb.append(splitColumn);
            sb.append("), MAX(");
            sb.append(splitColumn);
            sb.append("), COUNT(");
            sb.append(splitColumn);
            sb.append(")");
            sb.append(m.group(0));

            ResultSetHandler<long[]> handler = new ResultSetHandler<long[]>() {

                @Override
                public long[] handle(ResultSet rs) throws SQLException {
                    long[] minAndMax = new long[3];
                    while (rs.next()) {
                        minAndMax[0] = rs.getLong(1);
                        minAndMax[1] = rs.getLong(2);
                        minAndMax[2] = rs.getLong(3);
                    }
                    return minAndMax;
                }
            };

            QueryRunner runner = new QueryRunner();
            return runner.query(conn, sb.toString(), handler);
        }

        return minAndMax;
    }

    /**
     * 查询表数值类型的主键
     *
     * @param conn
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     */
    public static String getDigitalPrimaryKey(Connection conn, String catalog, String schema, String table, String keywordEscaper)
            throws SQLException {
        List<String> primaryKeys = new ArrayList<String>();
        ResultSet rs = conn.getMetaData().getPrimaryKeys(catalog, schema, table);
        while (rs.next()) {
            primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
        rs.close();

        if (primaryKeys.size() > 0) {
            Map<String, Integer> map = getColumnTypes(conn, table, keywordEscaper);
            for (String pk : primaryKeys) {
                if (isDigitalType(map.get(pk.toLowerCase()))) {
                    return pk;
                }
            }
        }

        return null;
    }

    /**
     * 判断字段类型是否为数值类型
     *
     * @param sqlType
     * @return
     */
    public static boolean isDigitalType(int sqlType) {
        switch (sqlType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;

            default:
                return false;
        }
    }

    public static boolean isStringType(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
                return true;

            default:
                return false;
        }
    }

    public static boolean isClobType(int sqlType) {
        switch (sqlType) {
            case Types.CLOB:
                return true;
            default:
                return false;
        }
    }

    public static boolean isBlobType(int sqlType) {
        switch (sqlType) {
            case Types.BLOB:
                return true;
            default:
                return false;
        }
    }

    public static Connection getConnection(String driverClassName, String url, String username, String password) throws Exception {
        Class.forName(driverClassName);
        return DriverManager.getConnection(url, username, password);
    }

    public static void main(String[] args) {

        try {
            Connection conn = JdbcUtils.getConnection("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@node6:1521:orcl", "carpo", "123456");
            String str = getMaxValue(conn, "SELECT * from TEST_TYPE", "U");
            System.out.println(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
