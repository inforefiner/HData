package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import com.github.stuxuhai.hdata.plugin.jdbc.JdbcUtils;

import java.util.ArrayList;
import java.util.List;

public class JDBCIterator {

    //private static final Logger LOG = LogManager.getLogger(JDBCIterator.class);

    private List<JdbcUnit> unitList = new ArrayList<JdbcUnit>();
    private Integer length = 0;
    private Integer current = 0;

    public void add(JdbcUnit unit) {
        unitList.add(unit);
        length++;
    }

    /**
     * 获取 下一条 SQL
     */
    public synchronized String getNextSQL(int seq) {
        if (current >= length) {
            return null;
        }
        String sql = unitList.get(current).getNextSQL(seq);
        if (sql == null) {
            current++;
        } else {
            return sql;
        }
        return getNextSQL(seq);
    }

    protected static class JdbcUnit {
        private long startCursor;
        private long endCursor;
        private long start;
        private long end;
        private long step;
        private int parallelism;
        private int middle;
        private String splitColumn;
        private String selectColumn;
        private String sql;
        private String driver;
        private String table;
        private String condition;

        public JdbcUnit(String driver, String sql, String condition, String selectColumn, String splitColumn, String table, long start, long end, long step, int parallelism) {
            this.driver = driver;
            this.sql = sql;
            this.condition = condition;
            this.splitColumn = splitColumn;
            this.selectColumn = selectColumn;
            this.table = table;
            this.start = start;
            this.end = end;
            this.step = step;

            this.startCursor = start;
            this.endCursor = end;
            this.parallelism = parallelism;

            this.middle = (int) Math.ceil(parallelism / 2);
        }

        public String getNextSQL(int seq) {
            if (startCursor >= endCursor) {
                return null;
            }

            long tempStart, tempEnd;

            tempStart = startCursor;

            if (step <= 0 || startCursor + step > endCursor) {
                tempEnd = endCursor;
            } else {
                tempEnd = startCursor + step;
            }

            startCursor = tempEnd;

            String currentSql = sql;

            if (splitColumn != null && !splitColumn.isEmpty() && !JdbcUtils.isSqlServer(driver)) {
                currentSql += " ORDER BY " + splitColumn;
            }

            String pagingSql = "";

            if (JdbcUtils.isOracle(driver)) {
                pagingSql = "SELECT " + selectColumn + " FROM ( SELECT ROWNUM AS RN, T.* FROM (" + currentSql.replace(JDBCSplitter.CONDITIONS, condition) + ") T WHERE ROWNUM <= " + tempEnd + ") T2 WHERE T2.RN > " + tempStart;
            } else if (JdbcUtils.isDB2(driver)) {
                pagingSql = "SELECT " + selectColumn + " FROM (" + currentSql.replace(JDBCSplitter.CONDITIONS, condition) + ") WHERE RN > " + tempStart + " and RN <= " + tempEnd;
            } else if (JdbcUtils.isPG(driver)) {
                pagingSql = currentSql.replace(JDBCSplitter.CONDITIONS, condition) + " LIMIT " + step + " OFFSET " + tempStart;
            } else if (JdbcUtils.isSqlServer(driver)) {
                currentSql = currentSql.replace("SELECT", "SELECT TOP " + step);
                pagingSql = currentSql.replace(JDBCSplitter.CONDITIONS, splitColumn + " NOT IN (SELECT TOP " + tempStart + " " + splitColumn + " FROM " + table + ")");
            } else {
                pagingSql = currentSql.replace(JDBCSplitter.CONDITIONS, condition) + " LIMIT " + tempStart + "," + step;
            }

            return pagingSql;
        }

        @Override
        public String toString() {
            return "JdbcUnit{" +
                    "startCursor=" + startCursor +
                    ", endCursor=" + endCursor +
                    ", start=" + start +
                    ", end=" + end +
                    ", step=" + step +
                    ", parallelism=" + parallelism +
                    ", middle=" + middle +
                    ", splitColumn='" + splitColumn + '\'' +
                    ", selectColumn='" + selectColumn + '\'' +
                    ", sql='" + sql + '\'' +
                    ", driver='" + driver + '\'' +
                    ", table='" + table + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {

        //SELECT SQL SELECT ID,FLOW_EXEC_ID,CHANNEL_KEY,NAME,CDO_NAME,PATH,INPUTSIZE,RECORDS,INPUTINDEX FROM CM_CARPO_FLOW_INPUT WHERE ID >= 833560100 AND ID < 833570100

        String sql = "SELECT Sync,Length,Type,Subtype,Protocol,Status_Control,Server_ID,Report_Time,Device,Channel,ID,Start_Time,Interface,xDR_ID,RAT,IMSI,IMEI,MSISDN,Procedure_Type,SubProcedure_Type,Procedure_Start_Time,Procedure_End_Time,Procedure_Status,Failure_Cause,Keyword,Mme_Ue_S1ap_Id,MME_Group_ID,MME_Code,M_TMSI,USER_IPv4,USER_IPv6_Len,USER_IPv6,Machine_IP_Addr_type,MME_IP_Addr,eNB_IP_Addr,MME_Port,eNB_Port,TAC,ECI,OLD_TAC,OLD_ECI,APN,EPS_Bearer_Number,Bearer_1_ID,Bearer_1_Type,Bearer_1_QCI,Bearer_1_Status,Bearer_1_eNB_GTP_TEID,Bearer_1_SGW_GTP_TEID,Bearer_2_ID,Bearer_2_Type,Bearer_2_QCI,Bearer_2_Status,Bearer_2_eNB_GTP_TEID,Bearer_2_SGW_GTP_TEID,Bearer_3_ID,Bearer_3_Type,Bearer_3_QCI,Bearer_3_Status,Bearer_3_eNB_GTP_TEID,Bearer_3_SGW_GTP_TEID,Bearer_4_ID,Bearer_4_Type,Bearer_4_QCI,Bearer_4_Status,Bearer_4_eNB_GTP_TEID,Bearer_4_SGW_GTP_TEID,Bearer_5_ID,Bearer_5_Type,Bearer_5_QCI,Bearer_5_Status,Bearer_5_eNB_GTP_TEID,Bearer_5_SGW_GTP_TEID,Bearer_6_ID,Bearer_6_Type,Bearer_6_QCI,Bearer_6_Status,Bearer_6_eNB_GTP_TEID,Bearer_6_SGW_GTP_TEID,Bearer_7_ID,Bearer_7_Type,Bearer_7_QCI,Bearer_7_Status,Bearer_7_eNB_GTP_TEID,Bearer_7_SGW_GTP_TEID,Bearer_8_ID,Bearer_8_Type,Bearer_8_QCI,Bearer_8_Status,Bearer_8_eNB_GTP_TEID,Bearer_8_SGW_GTP_TEID,CSFB_response,CNDomain FROM MUL_SCHEMA_new WHERE " + JDBCSplitter.CONDITIONS;
        JdbcUnit jdbcUnit = new JdbcUnit("xxx.oracle.xxx", sql, "(1 = 1)", "*", "", "",0, 2270000, 10000, 4);
        String str = null;
        int i = 0;
        while ((str = jdbcUnit.getNextSQL(i)) != null) {
            System.out.println(str);
            i++;
        }
    }
}
