package com.linkedin.thirdeye.datalayer.util;

import java.lang.reflect.Array;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractIndexEntity;

public class SqlQueryBuilder {


  private static final String BASE_ID = "base_id";
  //insert sql per table
  Map<String, String> insertSqlMap = new HashMap<>();
  private static final String NAME_REGEX = "[a-z][_a-z0-9]*";

  private static final String PARAM_REGEX = ":(" + NAME_REGEX + ")";

  private static final Pattern PARAM_PATTERN =
      Pattern.compile(PARAM_REGEX, Pattern.CASE_INSENSITIVE);
  private static Set<String> AUTO_UPDATE_COLUMN_SET =
      Sets.newHashSet("id", "last_modified", "update_time");

  private EntityMappingHolder entityMappingHolder;;

  public SqlQueryBuilder(EntityMappingHolder entityMappingHolder) {
    this.entityMappingHolder = entityMappingHolder;

  }

  public static String generateInsertSql(String tableName,
      LinkedHashMap<String, ColumnInfo> columnInfoMap) {

    StringBuilder values = new StringBuilder(" VALUES");
    StringBuilder names = new StringBuilder("");
    names.append("(");
    values.append("(");
    String delim = "";
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnName = columnInfo.columnNameInDB;
      if (columnInfo.field != null && !AUTO_UPDATE_COLUMN_SET.contains(columnName.toLowerCase())) {
        names.append(delim);
        names.append(columnName);
        values.append(delim);
        values.append("?");
        delim = ",";
      } else {
        System.out.println("Skipping column " + columnName + " from insert");
      }
    }
    names.append(")");
    values.append(")");

    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(tableName).append(names.toString()).append(values.toString());
    return sb.toString();
  }

  public PreparedStatement createInsertStatement(Connection conn, AbstractEntity entity)
      throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    return createInsertStatement(conn, tableName, entity);
  }

  public PreparedStatement createInsertStatement(Connection conn, String tableName,
      AbstractEntity entity) throws Exception {
    if (!insertSqlMap.containsKey(tableName)) {
      String insertSql = generateInsertSql(tableName,
          entityMappingHolder.columnInfoPerTable.get(tableName.toLowerCase()));
      insertSqlMap.put(tableName, insertSql);
      System.out.println(insertSql);
    }

    String sql = insertSqlMap.get(tableName);
    PreparedStatement preparedStatement =
        conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    int parameterIndex = 1;
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      if (columnInfo.field != null
          && !AUTO_UPDATE_COLUMN_SET.contains(columnInfo.columnNameInDB.toLowerCase())) {
        Object val = columnInfo.field.get(entity);
        System.out.println("Setting value:" + val + " for " + columnInfo.columnNameInDB);
        if (val != null) {
          if (columnInfo.sqlType == Types.CLOB) {
            Clob clob = conn.createClob();
            clob.setString(1, val.toString());
            preparedStatement.setClob(parameterIndex++, clob);
          } else {
            preparedStatement.setObject(parameterIndex++, val.toString(), columnInfo.sqlType);
          }

        } else {
          preparedStatement.setNull(parameterIndex++, columnInfo.sqlType);
        }
      }
    }
    return preparedStatement;
  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Long id) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName + " where id=?";
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    prepareStatement.setLong(1, id);
    return prepareStatement;
  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, List<Long> ids) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    StringBuilder sql = new StringBuilder("Select * from " + tableName + " where id IN (");
    String delim = "";
    for (Long id : ids) {
      sql.append(delim).append(id);
      delim = ", ";
    }
    sql.append(")");
    PreparedStatement prepareStatement = connection.prepareStatement(sql.toString());
    return prepareStatement;
  }

  public PreparedStatement createFindByParamsStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Map<String, Object> filters) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    LinkedHashMap<String, Object> parametersMap = new LinkedHashMap<>();
    if (filters != null && !filters.isEmpty()) {
      StringBuilder whereClause = new StringBuilder(" WHERE ");
      String delim = "";
      for (String columnName : filters.keySet()) {
        String dbFieldName = entityNameToDBNameMapping.get(columnName);
        whereClause.append(delim).append(dbFieldName).append("=").append("?");
        parametersMap.put(dbFieldName, filters.get(columnName));
        delim = " AND ";
      }
      sqlBuilder.append(whereClause.toString());
    }
    System.out.println("FIND BY SQL:" + sqlBuilder.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Entry<String, Object> paramEntry : parametersMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      System.out.println(
          "Setting parameter:" + parameterIndex + " to " + paramEntry.getValue().toString());
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue().toString(), info.sqlType);
    }
    return prepareStatement;
  }

  public PreparedStatement createUpdateStatement(Connection connection, AbstractEntity entity,
      Set<String> fieldsToUpdate) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);

    StringBuilder sqlBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
    String delim = "";
    LinkedHashMap<String, Object> parameterMap = new LinkedHashMap<>();
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnNameInDB = columnInfo.columnNameInDB;
      if (!AUTO_UPDATE_COLUMN_SET.contains(columnNameInDB)
          && (fieldsToUpdate == null || fieldsToUpdate.contains(columnInfo.columnNameInEntity))) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (Enum.class.isAssignableFrom(val.getClass())) {
            val = val.toString();
          }
          sqlBuilder.append(delim);
          sqlBuilder.append(columnNameInDB);
          sqlBuilder.append("=");
          sqlBuilder.append("?");
          delim = ",";
          System.out.println("Setting value:" + val + " for " + columnInfo.columnNameInDB);
          parameterMap.put(columnNameInDB, val);
        }
      }
    }
    //ADD WHERE CLAUSE TO CHECK FOR ENTITY ID
    sqlBuilder.append(" WHERE id=?");
    parameterMap.put("id", entity.getId());
    System.out.println("Update statement:" + sqlBuilder.toString());
    int parameterIndex = 1;
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    for (Entry<String, Object> paramEntry : parameterMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }

  public PreparedStatement createDeleteStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, List<Long> ids) throws Exception {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("ids to delete cannot be null/empty");
    }
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE  id IN( ");
    String delim = "";
    for (Long id : ids) {
      whereClause.append(delim).append(id);
    }
    whereClause.append(")");
    sqlBuilder.append(whereClause.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    return prepareStatement;
  }

  public PreparedStatement createDeleteByIdStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Map<String, Object> filters) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    LinkedHashMap<String, Object> parametersMap = new LinkedHashMap<>();
    for (String columnName : filters.keySet()) {
      String dbFieldName = entityNameToDBNameMapping.get(columnName);
      whereClause.append(dbFieldName).append("=").append("?");
      parametersMap.put(dbFieldName, filters.get(columnName));
    }
    sqlBuilder.append(whereClause.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Entry<String, Object> paramEntry : parametersMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }



  public PreparedStatement createFindAllStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName;
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    return prepareStatement;
  }

  public PreparedStatement createFindByParamsStatement(Connection connection,
      Class<? extends AbstractEntity> entityClass, Predicate predicate) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    List<Pair<String, Object>> parametersList = new ArrayList<>();
    generateWhereClause(entityNameToDBNameMapping, predicate, parametersList, whereClause);
    sqlBuilder.append(whereClause.toString());
    System.out.println("createFindByParamsStatement Query " + sqlBuilder.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (Pair<String, Object> pair : parametersList) {
      String dbFieldName = pair.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, pair.getValue(), info.sqlType);
      System.out.println("Setting " + pair.getKey() + " to " + pair.getValue());
    }
    return prepareStatement;
  }

  private void generateWhereClause(BiMap<String, String> entityNameToDBNameMapping,
      Predicate predicate, List<Pair<String, Object>> parametersList, StringBuilder whereClause) {
    String columnName = null;

    if (predicate.getLhs() != null) {
      columnName = entityNameToDBNameMapping.get(predicate.getLhs());
    }
    switch (predicate.getOper()) {
      case AND:
      case OR:
        whereClause.append("(");
        String delim = "";
        for (Predicate childPredicate : predicate.getChildPredicates()) {
          whereClause.append(delim);
          generateWhereClause(entityNameToDBNameMapping, childPredicate, parametersList,
              whereClause);
          delim = "  " + predicate.getOper().toString() + " ";
        }
        whereClause.append(")");
        break;
      case EQ:
      case GT:
      case LT:
      case NEQ:
      case LE:
      case GE:
        whereClause.append(columnName).append(predicate.getOper().toString()).append("?");
        parametersList.add(ImmutablePair.of(columnName, predicate.getRhs()));
        break;
      case IN:
        Object rhs = predicate.getRhs();
        if (rhs != null && rhs.getClass().isArray()) {
          whereClause.append(columnName).append(" ").append(Predicate.OPER.IN.toString())
              .append("(");
          delim = "";
          for (int i = 0; i < Array.getLength(rhs); i++) {
            whereClause.append(delim).append("?");
            parametersList.add(ImmutablePair.of(columnName, Array.get(rhs, i)));
            delim = ",";
          }
          whereClause.append(")");
        }
        break;
      case BETWEEN:
        whereClause.append(columnName).append(predicate.getOper().toString()).append("? AND ?");
        ImmutablePair<Object, Object> pair = (ImmutablePair<Object, Object>) predicate.getRhs();
        parametersList.add(ImmutablePair.of(columnName, pair.getLeft()));
        parametersList.add(ImmutablePair.of(columnName, pair.getRight()));
        break;
      default:
        throw new RuntimeException("Unsupported predicate type:" + predicate.getOper());

    }
  }

  public PreparedStatement createStatementFromSQL(Connection connection, String parameterizedSQL,
      Map<String, Object> parameterMap, Class<? extends AbstractEntity> entityClass)
          throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    parameterizedSQL = "select * from " + tableName + " " + parameterizedSQL;
    parameterizedSQL = parameterizedSQL.replace(entityClass.getSimpleName(), tableName);
    StringBuilder psSql = new StringBuilder();
    List<String> paramNames = new ArrayList<String>();
    Matcher m = PARAM_PATTERN.matcher(parameterizedSQL);

    int index = 0;
    while (m.find(index)) {
      psSql.append(parameterizedSQL.substring(index, m.start()));
      String name = m.group(1);
      index = m.end();
      if (parameterMap.containsKey(name)) {
        psSql.append("?");
        paramNames.add(name);
      } else {
        throw new IllegalArgumentException(
            "Unknown parameter '" + name + "' at position " + m.start());
      }
    }

    // Any stragglers?
    psSql.append(parameterizedSQL.substring(index));
    String sql = psSql.toString();
    BiMap<String, String> dbNameToEntityNameMapping =
        entityMappingHolder.columnMappingPerTable.get(tableName);
    for (Entry<String, String> entry : dbNameToEntityNameMapping.entrySet()) {
      String dbName = entry.getKey();
      String entityName = entry.getValue();
      sql = sql.toString().replaceAll(entityName, dbName);
    }
    System.out.println("Generated SQL:" + sql);
    PreparedStatement ps = connection.prepareStatement(sql);
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfo =
        entityMappingHolder.columnInfoPerTable.get(tableName);
    for (String entityFieldName : paramNames) {
      String dbFieldName = dbNameToEntityNameMapping.inverse().get(entityFieldName);

      Object val = parameterMap.get(entityFieldName);
      if (Enum.class.isAssignableFrom(val.getClass())) {
        val = val.toString();
      }
      System.out.println("Setting " + dbFieldName + " to " + val);
      ps.setObject(parameterIndex++, val, columnInfo.get(dbFieldName).sqlType);
    }

    return ps;
  }

  public PreparedStatement createUpdateStatementForIndexTable(Connection connection,
      AbstractIndexEntity entity) throws Exception {
    String tableName =
        entityMappingHolder.tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap =
        entityMappingHolder.columnInfoPerTable.get(tableName);

    StringBuilder sqlBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
    String delim = "";
    LinkedHashMap<String, Object> parameterMap = new LinkedHashMap<>();
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnNameInDB = columnInfo.columnNameInDB;
      if (!columnNameInDB.equalsIgnoreCase(BASE_ID)
          && !AUTO_UPDATE_COLUMN_SET.contains(columnNameInDB)) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (Enum.class.isAssignableFrom(val.getClass())) {
            val = val.toString();
          }
          sqlBuilder.append(delim);
          sqlBuilder.append(columnNameInDB);
          sqlBuilder.append("=");
          sqlBuilder.append("?");
          delim = ",";
          parameterMap.put(columnNameInDB, val);
        }
      }
    }
    //ADD WHERE CLAUSE TO CHECK FOR ENTITY ID
    sqlBuilder.append(" WHERE base_id=?");
    parameterMap.put(BASE_ID, entity.getBaseId());
    System.out.println("Update statement:" + sqlBuilder.toString());
    int parameterIndex = 1;
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    for (Entry<String, Object> paramEntry : parameterMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      System.out.println("Setting value:" + paramEntry.getValue() + " for " + dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }

//  public static void main(String[] args) {
//    EntityMappingHolder entityMappingHolder = new EntityMappingHolder();
//    SqlQueryBuilder builder = new SqlQueryBuilder(entityMappingHolder);
//    BiMap<String, String> entityNameToDBNameMapping;
//    Predicate startTimePredicate;
//    long startTime;
//    long endTime = "";
//    long functionId = "1";
//    startTimePredicate =
//        Predicate.AND(Predicate.GE("startTime", startTime), Predicate.LE("startTime", endTime));
//    Predicate endTimeTimePredicate;
//    endTimeTimePredicate =
//        Predicate.AND(Predicate.GE("endTime", startTime), Predicate.LE("endTime", endTime));;
//
//    Predicate functionIdPredicate = Predicate.EQ("functionId", functionId);
//    Predicate finalPredicate =
//        Predicate.AND(functionIdPredicate, Predicate.OR(endTimeTimePredicate, startTimePredicate));
//    builder.generateWhereClause(entityNameToDBNameMapping, finalPredicate, parametersMap,
//        whereClause);
//
//  }
}
