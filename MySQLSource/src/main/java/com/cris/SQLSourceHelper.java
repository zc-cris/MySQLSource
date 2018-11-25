package com.cris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * TODO
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class SQLSourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);
    //为定义的变量赋值（默认值），可在flume任务的配置文件中修改
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_START_VALUE = 0;
    private static final int DEFAULT_MAX_ROWS = 2000;
    private static final String DEFAULT_COLUMNS_SELECT = "*";
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";
    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static String connectionURL, connectionUserName, connectionPassword;

    //加载静态资源
    static {
        Properties p = new Properties();
        try {
            p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            connectionURL = p.getProperty("dbUrl");
            connectionUserName = p.getProperty("dbUser");
            connectionPassword = p.getProperty("dbPassword");
            Class.forName(p.getProperty("dbDriver"));
        } catch (IOException | ClassNotFoundException e) {
            LOG.error(e.toString());
        }
    }

    private int runQueryDelay, //两次查询的时间间隔
            startFrom,            //开始id
            currentIndex,         //当前id
            recordSixe = 0,      //每次查询返回结果的条数
            maxRow;                //每次查询的最大条数
    private String table,       //要操作的表
            columnsToSelect,     //用户传入的查询的列
            customQuery,          //用户传入的查询语句
            query,                 //构建的查询语句
            defaultCharsetResultSet;//编码集
    //上下文，用来获取配置文件
    private Context context;

    /**
     * 构造方法里初始化配置（通过读取 Flume 任务配置文件的参数）以及调用参数校验方法
     *
     * @param context
     */
    @SuppressWarnings("unused")
    SQLSourceHelper(Context context) {
        //初始化上下文
        this.context = context;

        //有默认值参数：获取flume任务配置文件中的参数，读不到的采用默认值
        this.columnsToSelect = context.getString("columns.to.select", DEFAULT_COLUMNS_SELECT);
        this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        this.startFrom = context.getInteger("start.from", DEFAULT_START_VALUE);
        this.defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

        //无默认值参数：获取flume任务配置文件中的参数，如果Flume 配置文件中没有配置的参数，那么就参考static 代码快读取的jdbc.properties 的配置
        this.table = context.getString("table");
        this.customQuery = context.getString("custom.query");
        connectionURL = context.getString("connection.url");
        connectionUserName = context.getString("connection.user");
        connectionPassword = context.getString("connection.password");
        conn = initConnection(connectionURL, connectionUserName, connectionPassword);

        //校验相应的配置信息，如果没有默认值的参数也没赋值，抛出异常
        try {
            checkMandatoryProperties();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        //获取当前表数据保存到 flume_data 表的id
        currentIndex = getStatusDBIndex(startFrom);
        //构建查询语句,需要去对应的表根据 id 查询数据
        query = buildQuery();
    }

    /**
     * 获取JDBC连接
     *
     * @param url
     * @param user
     * @param pw
     * @return JDBC 连接
     */
    private static Connection initConnection(String url, String user, String pw) {
        try {
            Connection conn = DriverManager.getConnection(url, user, pw);
            if (conn == null)
                throw new SQLException();
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 校验相应的配置信息（表，查询语句以及数据库连接的参数）
     *
     * @throws ConfigurationException
     */
    private void checkMandatoryProperties() throws ConfigurationException {
        if (table == null) {
            throw new ConfigurationException("property table not set");
        }
        if (connectionURL == null) {
            throw new ConfigurationException("connection.url property not set");
        }
        if (connectionUserName == null) {
            throw new ConfigurationException("connection.user property not set");
        }
        if (connectionPassword == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }


    /**
     * 构建sql语句：从 flume_meta 表获取指定表的 offset，然后构建 sql 从指定表去查新插入的数据
     *
     * @return 去指定表查询数据的 sql 语句
     */
    private String buildQuery() {
        String sql;
        StringBuilder execSql = new StringBuilder();
        //获取当前id
        currentIndex = getStatusDBIndex(startFrom);
        LOG.info(currentIndex + "what is the currentIndex？");
        if (customQuery == null) {
            // 如果 customerQuery 为null，就以 offsest 作为 id
            sql = "SELECT " + columnsToSelect + " FROM " + table;
            execSql.append(sql).append(" where ").append("id").append(">").append(currentIndex);
        } else {
             /*
                如果 customQuery 不为 null，那么就要将其最后已存在的 offset 替换掉新查询出的 offset（！！！）
             */
            sql = customQuery.substring(0, customQuery.indexOf(">") + 1) + currentIndex;
            execSql.append(sql);
        }
        return execSql.toString();

    }


    /**
     * 执行查询：从指定的表去查询数据
     *
     * @return 数据集
     */
    List<List<Object>> executeQuery() {
        try {
            //每次执行查询时都要重新生成sql，因为id不同
            customQuery = buildQuery();
            //存放结果的集合
            List<List<Object>> results = new ArrayList<>();
            if (ps == null) {
                ps = conn.prepareStatement(customQuery);
            }
            ResultSet result = ps.executeQuery(customQuery);
            while (result.next()) {
                //存放一条数据的集合（多个列）
                List<Object> row = new ArrayList<>();
                //将返回结果放入集合
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }
                results.add(row);
            }
            LOG.info("execSql:" + customQuery + "\nresultSize:" + results.size());
            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());
            // 重新连接
            conn = initConnection(connectionURL, connectionUserName, connectionPassword);
        }
        return null;
    }


    /**
     * 将结果集转化为字符串，每一条数据是一个list集合，将每一个小的list集合转化为字符串
     *
     * @param queryResult 结果集
     * @return 查询的结果集转换后的字符串
     */
    List<String> getAllRows(List<List<Object>> queryResult) {
        List<String> allRows = new ArrayList<>();
        if (queryResult == null || queryResult.isEmpty())
            return allRows;
        StringBuilder row = new StringBuilder();
        for (List<Object> rawRow : queryResult) {
            Object value;
            for (Object aRawRow : rawRow) {
                value = aRawRow;
                if (value == null) {
                    row.append(",");
                } else {
                    row.append(aRawRow.toString()).append(",");
                }
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }
        return allRows;
    }


    /**
     * 更新offset元数据状态，每次返回结果集后调用。必须记录每次查询的offset值，为程序每次查询使用，以id为offset
     *
     * @param size 新的 offset
     */
    void updateOffset2DB(int size) {
        //以source_tab做为KEY，（on duplicate key update 函数）如果 offset 不存在则插入，存在则更新（每个源表对应一条记录，记录当前表的 offset）
        String sql = "insert into flume_meta(source_tab,currentIndex) VALUES('"
                + this.table
                + "','" + (recordSixe += size)
                + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex)";
        LOG.info("updateStatus Sql:" + sql);
        execSql(sql);
    }


    /**
     * 执行sql语句：新增或者更新 flume_meta 表的数据
     *
     * @param sql
     */
    private void execSql(String sql) {
        try {
            ps = conn.prepareStatement(sql);
            LOG.info("exec::" + sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取当前 source_tab 的 offset 是多少, sql 语句中的 flume_meta 表写死了，实际上可以提取出来作为 Flume 的任务配置文件参数
     *
     * @param startFrom
     * @return offset
     */
    private Integer getStatusDBIndex(int startFrom) {
        //从flume_meta表中查询出当前表对应的 id 是多少，拿到这个 id 就可以去对应的表根据 id 查询数据并写入到 Flume 的 channel 里去
        String dbIndex = queryOne("select currentIndex from flume_meta where source_tab='" + table + "'");
        LOG.info("currentIndex = " + dbIndex);
        if (dbIndex != null) {
            return Integer.parseInt(dbIndex);
        }
        //如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初传入的值即可
        return startFrom;
    }


    /**
     * 具体查询 flume_meta 对应数据表的 offset 的方法
     *
     * @param sql
     * @return offset
     */
    private String queryOne(String sql) {
        ResultSet result;
        try {
            ps = conn.prepareStatement(sql);
            LOG.info("sql = " + sql);
            result = ps.executeQuery();
            if (result.next()) {
                return result.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 关闭相关资源
     */
    void close() {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
