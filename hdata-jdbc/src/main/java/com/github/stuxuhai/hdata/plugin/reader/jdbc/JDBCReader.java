package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.jdbc.JdbcUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.List;

public class JDBCReader extends Reader {

    private final Logger logger = LoggerFactory.getLogger(JDBCReader.class);

    private Connection connection;
    private JDBCIterator sqlPiece;
    private List<String> sqlList;

    private Fields fields = new Fields();
    private int columnCount;
    private int sequence;
    private int[] columnTypes;
    private String nullString = null;
    private String nullNonString = null;
    private String fieldWrapReplaceString = null;
    private DecimalFormat decimalFormat = null;

    private String url;
    private String driver;
    private String username;
    private String password;
    private String catalog;
    private String schema;

    private int queryTimeout;
    private int fetchSize;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        driver = readerConfig.getString(JDBCReaderProperties.DRIVER);
        url = readerConfig.getString(JDBCReaderProperties.URL);
        username = readerConfig.getString(JDBCReaderProperties.USERNAME);
        password = readerConfig.getString(JDBCReaderProperties.PASSWORD);
        catalog = readerConfig.getString(JDBCReaderProperties.CATALOG);
        schema = readerConfig.getString(JDBCReaderProperties.SCHEMA);

        nullString = readerConfig.getString(JDBCReaderProperties.NULL_STRING);
        nullNonString = readerConfig.getString(JDBCReaderProperties.NULL_NON_STRING);
        fieldWrapReplaceString = readerConfig.getProperty(JDBCReaderProperties.FIELD_WRAP_REPLACE_STRING);

        String numberFormat = readerConfig.getProperty(JDBCReaderProperties.NUMBER_FORMAT);
        if (numberFormat != null) {
            decimalFormat = new DecimalFormat(numberFormat);
        }

        sqlPiece = (JDBCIterator) readerConfig.get(JDBCReaderProperties.SQL_ITERATOR);
        sqlList = (List<String>) readerConfig.get(JDBCReaderProperties.SQL);
        if (sqlPiece != null) {
            sequence = (Integer) readerConfig.get(JDBCReaderProperties.SQL_SEQ);
        }

        queryTimeout = readerConfig.getInt(JDBCReaderProperties.QUERY_TIMEOUT, 0);
        fetchSize = readerConfig.getInt(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 0);
    }

    private Connection getConnection() throws HDataException {
        try {
            Connection connection = JdbcUtils.getConnection(driver, url, username, password);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            if (StringUtils.isNotBlank(catalog)) {
                connection.setCatalog(catalog);
            }
            if (StringUtils.isNotBlank(schema)) {
                connection.setSchema(schema);
            }
            return connection;
        } catch (Throwable e) {
            logger.error("can't get connection with " + url, e);
        }
        throw new HDataException("can't get connection with " + url);
    }

    private synchronized Statement getStatement() {
        try {
            if (connection == null || connection.isClosed() || !connection.isValid(30)) {
                connection = getConnection();
            }

            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setQueryTimeout(queryTimeout);
            statement.setFetchSize(fetchSize);
            logger.info("create statement success, query timeout = {}, fetch size = {}", queryTimeout, fetchSize);
            return statement;
        } catch (Throwable e) {
            logger.error("can't create statement", e);
        }
        throw new HDataException("can't create statement");
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        int total = 0;
        try {
            if (sqlPiece != null) {
                while (true) {
                    String sql = sqlPiece.getNextSQL(sequence);
                    if (sql == null) {
                        break;
                    }
                    total += executeSingle(sql, recordCollector);
                }
            } else if (sqlList != null && sqlList.size() > 0) {
                for (String sql : sqlList) {
                    total += executeSingle(sql, recordCollector);
                }
            } else {
                throw new HDataException("sql 分片 为空");
            }
        } catch (Throwable e) {
            logger.error("JdbcReader execute error", e);
            throw new HDataException(e);
        }
        logger.info("reader execute done, total reads =  {} ", total);
    }

    private int executeSingle(String sql, RecordCollector recordCollector) throws Throwable {
        int rows = 0;
        long startTime = System.currentTimeMillis();
        Statement statement = getStatement();
        ResultSet rs = null;
        try {
            logger.info("execute query sql =  {} ", sql);
            rs = statement.executeQuery(sql);
            logger.info("execute query sql = {} done, fetch size = {}", sql, rs.getFetchSize());
        } catch (Throwable e) {
            logger.error("execute query error", e);
            throw e;
        }
        try {
            if (columnCount == 0 || columnTypes == null) {
                ResultSetMetaData metaData = rs.getMetaData();
                columnCount = metaData.getColumnCount();
                columnTypes = new int[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    fields.add(metaData.getColumnName(i));
                    columnTypes[i - 1] = metaData.getColumnType(i);
                    logger.info("column: " + metaData.getColumnName(i) + " type: " + metaData.getColumnType(i));
                }
            }

            while (rs.next()) {
                Record r = new DefaultRecord(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object o = null;
                    try {
                        o = rs.getObject(i);
                    } catch (Throwable e) {
                        o = "";
                    }
                    if (o != null && JdbcUtils.isClobType(columnTypes[i - 1])) {
                        Clob clob = (Clob) o;
                        try {
                            o = clob.getSubString(1, (int) clob.length());
                        } catch (Throwable e) {
                            o = "";
                        }
                    }
                    if (o != null && JdbcUtils.isNclobType(columnTypes[i - 1])) {
                        NClob nClob = (NClob) o;
                        try {
                            o = nClob.getSubString(1, (int) nClob.length());
                        } catch (Throwable e) {
                            o = "";
                        }
                    }
                    if (o != null && JdbcUtils.isBinaryType(columnTypes[i - 1])) {
                        o = rs.getBytes(i);
                    }
                    if (o == null && nullString != null && JdbcUtils.isStringType(columnTypes[i - 1])) {
                        r.add(i - 1, nullString);
                    } else if (o == null && nullNonString != null && !JdbcUtils.isStringType(columnTypes[i - 1])) {
                        r.add(i - 1, nullNonString);
                    } else if (o instanceof String && fieldWrapReplaceString != null) {
                        r.add(i - 1, ((String) o).replace("\r\n", fieldWrapReplaceString).replace("\n", fieldWrapReplaceString));
                    } else {
                        if (decimalFormat != null) {
                            if (o instanceof Double) {
                                r.add(i - 1, Double.valueOf(decimalFormat.format(o)));
                            } else if (o instanceof Float) {
                                r.add(i - 1, Float.valueOf(decimalFormat.format(o)));
                            } else {
                                r.add(i - 1, o);
                            }
                        } else {
                            r.add(i - 1, o);
                        }
                    }
                }
                recordCollector.send(r);
                rows++;
            }
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 1000 * 10) {
                logger.info("execute select sql =  {} , rows = {}, use time = {}", sql, rows, cost);
            }
        } catch (Throwable e) {
            if (e instanceof SQLException) {
                logger.error("sql error with error code {}", ((SQLException) e).getErrorCode(), e);
            } else {
                logger.error("execute sql error", e);
            }
            throw e;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return rows;
    }

    @Override
    public void close() {
        DbUtils.closeQuietly(connection);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Splitter newSplitter() {
        return new JDBCSplitter();
    }

}
