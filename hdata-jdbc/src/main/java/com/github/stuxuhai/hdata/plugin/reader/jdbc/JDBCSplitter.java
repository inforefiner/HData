package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.jdbc.JdbcUtils;
import com.github.stuxuhai.hdata.util.NumberUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JDBCSplitter extends Splitter {

    public static final String CONDITIONS = "$CONDITIONS";
    private static final Pattern PATTERN = Pattern.compile("^([a-zA-Z]\\w*)(\\[(\\d+)-(\\d+)\\])?(_(\\d+)_(\\d+)_(\\d+))?$");

    private static final Logger LOG = LogManager.getLogger(JDBCSplitter.class);

    private void checkIfContainsConditionKey(String sql, String errorMessage) {
        if (!sql.contains(CONDITIONS)) {
            LOG.error(errorMessage);
        }
    }

    private List<PluginConfig> buildPluginConfigs(Connection conn, List<String> sqlList, String splitColumn, PluginConfig readerConfig) throws Throwable {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        String driver = readerConfig.getString(JDBCReaderProperties.DRIVER);
        String table = readerConfig.getString(JDBCReaderProperties.TABLE);
        String columns = readerConfig.getString(JDBCReaderProperties.COLUMNS);
        int parallelism = readerConfig.getParallelism();
        long maxFetchSize = readerConfig.getLong(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 10000);
        JDBCIterator iterator = new JDBCIterator();
        for (String sql : sqlList) {
            long count = JdbcUtils.getCount(conn, sql.replace(CONDITIONS, "(1 = 1)"));
            if (count > 0) {
                long step = maxFetchSize;
                iterator.add(new JDBCIterator.JDBCUnit2(driver, sql, columns, splitColumn, table,0, count, step, parallelism));
            }
        }
        for (int i = 0; i < parallelism; i++) {
            PluginConfig otherReaderConfig = (PluginConfig) readerConfig.clone();
            otherReaderConfig.put(JDBCReaderProperties.SQL_ITERATOR, iterator);
            otherReaderConfig.put(JDBCReaderProperties.SQL_SEQ, i);
            list.add(otherReaderConfig);
        }
        return list;
    }

    private boolean isDateTimeType(String type) {
        if (StringUtils.isNotBlank(type)) {
            type = type.toUpperCase();
            if ((type.indexOf("DATE") > -1 || type.indexOf("TIME") > -1)) {
                return true;
            }
        }
        return false;
    }


    private void getCursorValue(String driver, String cursorType, String cursorVal, StringBuilder where) {
        if (this.isDateTimeType(cursorType)) {
            if (cursorVal.length() > 19) {
                cursorVal = cursorVal.substring(0, 19);
            }
            if (JdbcUtils.isOracle(driver)) {
                where.append("TO_DATE('" + cursorVal + "','yyyy-mm-dd hh24:mi:ss')");
            } else if (JdbcUtils.isSqlServer(driver)) {
                where.append("DATEADD(SECOND, 1, '" + cursorVal + "')");
            } else {
                where.append("'" + cursorVal + "'");
            }
        } else {
            where.append(cursorVal);
        }
    }

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String keywordEscaper = readerConfig.getProperty(JDBCReaderProperties.KEYWORD_ESCAPER, "");
        String driver = readerConfig.getString(JDBCReaderProperties.DRIVER);
        Preconditions.checkNotNull(driver, "JDBC reader required property: driver");

        String url = readerConfig.getString(JDBCReaderProperties.URL);
        Preconditions.checkNotNull(url, "JDBC reader required property: url");

        String username = readerConfig.getString(JDBCReaderProperties.USERNAME);
        String password = readerConfig.getString(JDBCReaderProperties.PASSWORD);
        int parallelism = readerConfig.getParallelism();
        int maxFetchSize = readerConfig.getInt(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 10000);
        String cursorColumn = readerConfig.getString(JDBCReaderProperties.CURSOR_COLUMN);
        String cursorType = readerConfig.getString(JDBCReaderProperties.CURSOR_TYPE);
        String cursorValue = readerConfig.getString(JDBCReaderProperties.CURSOR_VALUE);

        LOG.info("splitting cursorColumn = {}, cursorType = {}, cursorValue = {}, parallelism = {}", cursorColumn, cursorType, cursorValue, parallelism);

        List<String> sqlList = new ArrayList<String>();

        String table = readerConfig.getString(JDBCReaderProperties.TABLE);

        Preconditions.checkNotNull(table, "JDBC reader required property: table");

        LOG.info("building sql for table: {}", table);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        if (!readerConfig.containsKey(JDBCReaderProperties.COLUMNS) && !readerConfig.containsKey(JDBCReaderProperties.EXCLUDE_COLUMNS)) {
            sql.append("*");
        } else if (readerConfig.containsKey(JDBCReaderProperties.COLUMNS)) {
            String columns = readerConfig.getString(JDBCReaderProperties.COLUMNS);
            sql.append(columns);
        }

        Connection conn = null;

        try {
            conn = JdbcUtils.getConnection(driver, url, username, password);
            String catalog = null;
            try {
                catalog = conn.getCatalog();
            } catch (Throwable e) {
//                e.printStackTrace();
            }
            String schema = null;
            try {
                schema = conn.getSchema();
            } catch (Throwable e) {
//                e.printStackTrace();
            }
            String splitKey = JdbcUtils.getSplitKey(conn, catalog, schema, table);

            if (JdbcUtils.isOracle(driver)) {
                sql.append(", ROWNUM RN");
            }
            if (JdbcUtils.isDB2(driver)) {
                sql.append(", ROW_NUMBER() OVER() AS RN");
            }
//            if (JdbcUtils.isSqlServer(driver)) {
//                sql.append(", ROW_NUMBER() OVER(ORDER BY " + splitKey + " ASC) AS RN");
//            }

            sql.append(" FROM ");
            sql.append(keywordEscaper).append(table).append(keywordEscaper);

            sql.append(" WHERE ");
            sql.append(CONDITIONS);

            if (readerConfig.containsKey(JDBCReaderProperties.WHERE)) {
                String where = readerConfig.getString(JDBCReaderProperties.WHERE);
                sql.append(" AND ");
                sql.append(where);
            }

            String sqlExpr = sql.toString();

            if (StringUtils.isNotBlank(cursorColumn)) {
                if (StringUtils.isNotBlank(cursorValue)) {
                    sql.append(" AND ");
                    sql.append(cursorColumn);
                    sql.append(" > ");
                    getCursorValue(driver, cursorType, cursorValue, sql);
                }
                String newCursorValue = JdbcUtils.getMaxValue(conn, sqlExpr.replace(CONDITIONS, "(1 = 1)"), cursorColumn);
                if (newCursorValue != null && !newCursorValue.equals(cursorValue)) {
                    sql.append(" AND ");
                    sql.append(cursorColumn);
                    sql.append(" <= ");
                    getCursorValue(driver, cursorType, newCursorValue, sql);
                    jobConfig.setString("CursorValue", newCursorValue);
                }
            }

            sqlList.add(sql.toString());

            if (parallelism > 1 || maxFetchSize > 0) {
                LOG.info("table {} find digital primary key: {}", table, splitKey);
                return buildPluginConfigs(conn, sqlList, splitKey, readerConfig);
            }

            for (int i = 0; i < sqlList.size(); i++) {
                sqlList.set(i, sqlList.get(i).replace(CONDITIONS, "(1 = 1)"));
            }

            readerConfig.put(JDBCReaderProperties.SQL, sqlList);

        } catch (Throwable e) {
            throw new HDataException(e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        List<PluginConfig> readerConfigList = new ArrayList<PluginConfig>();
        readerConfigList.add(readerConfig);
        return readerConfigList;
    }

    /**
     * 表名是否 符合要求
     */
    public static Boolean isMatch(String content) {
        for (String piece : com.google.common.base.Splitter.on(",").omitEmptyStrings().trimResults().split(content)) {
            if (!PATTERN.matcher(piece).find()) {
                return false;
            }
        }

        return true;
    }

    /**
     * 内容解析成列表
     */
    public static List<String> getRange(String content) {
        // split to pieces and be unique.
        HashSet<String> hs = new HashSet<String>();
        for (String piece : com.google.common.base.Splitter.on(",").omitEmptyStrings().trimResults().split(content)) {
            hs.addAll(parseRange(piece));
        }

        List<String> range = new ArrayList<String>(hs);

        Collections.sort(range);

        return range;
    }

    /**
     * get the range
     * <p/>
     * 01-04 = 01,02,03,04
     */
    private static List<String> parseRange(String content) {
        Matcher matcher = PATTERN.matcher(content);
        List<String> pieces = new ArrayList<String>();

//        if (!matcher.find()) {
//            throw new RuntimeException(content + ": The format is wrong.");
//        }

        if (!content.contains("[")) {
            pieces.add(content);
            return pieces;
        }

        String prefix = matcher.group(1);
        String begin = matcher.group(3);
        String after = matcher.group(4);

        String format = "%0" + begin.length() + "d";

        int[] rangeList = NumberUtils.getRange(Integer.valueOf(begin), Integer.valueOf(after));

        for (int number : rangeList) {
            String suffix = matcher.group(5);
            if (suffix != null) {
                pieces.add(prefix + String.format(format, number) + suffix);
            } else {
                pieces.add(prefix + String.format(format, number));
            }
        }

        return pieces;
    }

    public static String getFirstTableName(String tableName) {
        return getRange(tableName).get(0);
    }

}
