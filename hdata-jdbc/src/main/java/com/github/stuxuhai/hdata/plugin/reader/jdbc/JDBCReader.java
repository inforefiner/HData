package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.jdbc.JdbcUtils;
import com.github.stuxuhai.hdata.plugin.jdbc.ParamUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.List;

public class JDBCReader extends Reader {

    private final Logger logger = LogManager.getLogger(JDBCReader.class);

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

        connection = getConnection();
        sqlPiece = (JDBCIterator) readerConfig.get(JDBCReaderProperties.SQL_ITERATOR);
        sqlList = (List<String>) readerConfig.get(JDBCReaderProperties.SQL);
        if (sqlPiece != null) {
            sequence = (Integer) readerConfig.get(JDBCReaderProperties.SQL_SEQ);
        }

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

    private Statement getStatement() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = getConnection();
            }
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            int queryTimeout = ParamUtils.getInt(JDBCReaderProperties.QUERY_TIMEOUT, 30);
            statement.setQueryTimeout(queryTimeout);
            int fetchSize = ParamUtils.getInt(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 100);
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
        try {
            Statement statement = getStatement();
            if (sqlPiece != null) {
                while (true) {
                    String sql = sqlPiece.getNextSQL(sequence);
                    if (sql == null) {
                        break;
                    }
                    executeSingle(statement, sql, recordCollector);
                }
            } else if (sqlList != null && sqlList.size() > 0) {
                for (String sql : sqlList) {
                    executeSingle(statement, sql, recordCollector);
                }
            } else {
                throw new HDataException("sql 分片 为空");
            }
            statement.close();
        } catch (Throwable e) {
            logger.error(e);
            throw new HDataException(e);
        }
    }

    private void executeSingle(Statement statement, String sql, RecordCollector recordCollector) throws Throwable {
        int rows = 0;
        long startTime = System.currentTimeMillis();
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
                }
            }
            while (rs.next()) {
                Record r = new DefaultRecord(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object o = null;
                    try {
                        o = rs.getObject(i);
                    } catch (SQLException e) {
                        o = "";
                    }
                    if (o != null && JdbcUtils.isClobType(columnTypes[i - 1])) {
                        Clob clob = (Clob) o;
                        try {
                            o = clob.getSubString(1, (int) clob.length());
                        } catch (SQLException e) {
                            o = "";
                        }
                    }
                    if (o != null && JdbcUtils.isBlobType(columnTypes[i - 1])) {
                        Blob blob = (Blob) o;
                        try {
                            o = new String(blob.getBytes(1, (int) blob.length()), "UTF8");
                        } catch (UnsupportedEncodingException e) {
                            o = "";
                        }
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
            logger.info("execute select sql =  {} , rows = {}, use time = {}", sql, rows, System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
            throw e;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
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
