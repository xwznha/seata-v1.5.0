/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.exec;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.core.context.RootContext;
import io.seata.core.model.BranchType;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.mysql.MySQLInsertOrUpdateExecutor;
import io.seata.rm.datasource.sql.SQLVisitorFactory;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.util.JdbcConstants;

/**
 * The type Execute template.
 *
 * @author sharajava
 */
public class ExecuteTemplate {

    /**
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        return execute(null, statementProxy, statementCallback, args);
    }

    /**
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param sqlRecognizers    the sql recognizer list
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(List<SQLRecognizer> sqlRecognizers,
                                                     StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        // RootContext ThreadLocal 中存了 xid 和是否需要 global lock, 前面介绍拦截器的时候,
        // 我并没有展示 GlobalLock 拦截器的代码,
        // 实际上它就是修改了一下标志位, 具体需不需要向 TC 确认锁, 是在 SQL 执行的时候才知道
        // 从这里我们可以看出, 只有当存在全局事务, 或者需要全局锁的时候, 才会加入 Seata 的流程,
        // 否则用默认的 statement 直接执行
        // !RootContext.inGlobalTransaction() 1.5.2版本
        if (!RootContext.requireGlobalLock() && BranchType.AT != RootContext.getBranchType()) {
            // Just work as original statement
            return statementCallback.execute(statementProxy.getTargetStatement(), args);
        }
        // 根据 db 的不同, 获取不同的分析器, 区分Mysql和Oracle
        String dbType = statementProxy.getConnectionProxy().getDbType();
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            sqlRecognizers = SQLVisitorFactory.get(
                    statementProxy.getTargetSQL(),
                    dbType);
        }
        Executor<T> executor;
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            // PlainExecutor 是 jdbc 原始执行器, 不包含 Seata 的逻辑
            executor = new PlainExecutor<>(statementProxy, statementCallback);
        } else {
            if (sqlRecognizers.size() == 1) {
                SQLRecognizer sqlRecognizer = sqlRecognizers.get(0);
                // 分析出 SQL 的类型, 调用不同的执行器, 这里我们会发现 Select 语句会直接用原生的 PlainExecutor 执行,
                // 也正是如此才说 Seata 默认执行在读未提交的隔离级别下(直接 select 查询, 并不会找 TC 确认锁的情况),
                // 正如前面说的, 读已提交隔离级别是通过 SELECT_FOR_UPDATE + GlobalLock 联合来实现
                // 到 ExecuteTemplate 这一层终于开始干实事了:
                // 1.判断是不是在全局事务中, 是不是有全局锁的需求
                // 2.分析 SQL, 只代理 Create, Update, Delete 过程和 SelectForUpdate
                // 3.调用最终采用的执行器
                // 在 ExecuteTemplate 有两个难点, 一个是 SQL 分析器, 另一个是 SQL 执行器,
                // 分析器就是根据不同数据库 SQL 的关键字构建抽象语法树, 然后得出该 SQL 是什么类型的,
                // 因为这部分大部分是语法分析, 比较繁杂, 而且 Seata 也是调用了另一个库(druid)提供的功能,
                // 我们这里就不展开介绍了。我们着重看一下 Seata 通过分析结果, 怎么做到无侵入的。
                // AbstractDMLBaseExecutor
                // protected T executeAutoCommitFalse(Object[] args) throws Exception {
                //    TableRecords beforeImage = beforeImage();
                //    T result = statementCallback.execute(statementProxy.getTargetStatement(), args);
                //    TableRecords afterImage = afterImage(beforeImage);
                //    prepareUndoLog(beforeImage, afterImage);
                //    return result;
                // }
                // 这里先看一下所有 SQL 执行器的基类 AbstractDMLBaseExecutor 的工作流程：
                // 1.获取执行前快照 2.执行原始SQL 3.获取执行后快照 4.准备回滚日志
                // 有一个需要注意的点是，上述的所有过程要在一个本地事务中完成，如果本地事务默认是自动提交的话，Seata 会先将其改为不自动提交，
                // 再开始上述过程。无论是Create,Update,还是Delete, 都是走这一个流程出来的,
                // 它们的不同点就在于beforeImage和afterImage 的实现。可以看到,Create过程的beforeImage是空,
                // afterImage 是先获取主键列表, 然后 buildTableRecords 构建查询 select * form table where pk in (pk list)。
                //
                //
                switch (sqlRecognizer.getSQLType()) {
                    case INSERT:
                        executor = EnhancedServiceLoader.load(InsertExecutor.class, dbType,
                                    new Class[]{StatementProxy.class, StatementCallback.class, SQLRecognizer.class},
                                    new Object[]{statementProxy, statementCallback, sqlRecognizer});
                        break;
                    case UPDATE:
                        executor = new UpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case DELETE:
                        executor = new DeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case SELECT_FOR_UPDATE:
                        executor = new SelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case INSERT_ON_DUPLICATE_UPDATE:
                        switch (dbType) {
                            case JdbcConstants.MYSQL:
                            case JdbcConstants.MARIADB:
                                executor =
                                    new MySQLInsertOrUpdateExecutor(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            default:
                                throw new NotSupportYetException(dbType + " not support to INSERT_ON_DUPLICATE_UPDATE");
                        }
                        break;
                    default:
                        executor = new PlainExecutor<>(statementProxy, statementCallback);
                        break;
                }
            } else {
                executor = new MultiExecutor<>(statementProxy, statementCallback, sqlRecognizers);
            }
        }
        T rs;
        try {
            rs = executor.execute(args);
        } catch (Throwable ex) {
            if (!(ex instanceof SQLException)) {
                // Turn other exception into SQLException
                ex = new SQLException(ex);
            }
            throw (SQLException) ex;
        }
        return rs;
    }

}
