package com.starrocks.sql.analyzer.masking;

import com.google.common.base.Joiner;
import com.starrocks.analysis.*;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.Relation;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName ColumnUtils
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 上午11:19
 */
public class ColumnUtils {
    /**
     * @param expr
     * @param columnAlias
     * @return
     */
    public static LinkedList<ColumnNode> buildColumnNodes(Expr expr, String columnAlias) {
        if (expr instanceof SlotRef) {
            LinkedList<ColumnNode> columnNodes = new LinkedList<>();
            columnNodes.add(buildColumnNode((SlotRef) expr, columnAlias));
            return columnNodes;
        } else if (expr instanceof CastExpr) {
            return buildColumnNodes(((CastExpr) expr).getChildren(), columnAlias);
        } else if (expr instanceof FunctionCallExpr) {
            return buildColumnNodes(((FunctionCallExpr) expr).getChildren(), columnAlias);
        } else if (expr instanceof ArithmeticExpr) {
            return buildColumnNodes(((ArithmeticExpr) expr).getChildren(), columnAlias);
        } else if (expr instanceof FieldReference) {
            FieldReference field = (FieldReference) expr;
            LinkedList<ColumnNode> columnNodes = new LinkedList<>();
            columnNodes.add(new ColumnNode().setName(field.getFieldIndex() + "").setAlias(columnAlias));
            return columnNodes;
        } else if (expr instanceof LiteralExpr) {
            return new LinkedList<>();
        }
        return new LinkedList<>();
    }

    /**
     * @param slotRef
     * @param columnAlias
     * @return
     */
    public static ColumnNode buildColumnNode(SlotRef slotRef, String columnAlias) {
        ColumnNode columnNode = new ColumnNode().setName(slotRef.getColumnName()).setAlias(columnAlias);
        setTableName(columnNode, slotRef.getTblNameWithoutAnalyzed(), false);
        return columnNode;
    }

    /**
     * @param columnNode
     * @param tableName
     * @param checkDataMasking
     */
    public static void setTableName(ColumnNode columnNode, TableName tableName, boolean checkDataMasking) {
        if (Objects.isNull(tableName)) return;
        // 判断字段是否需要加密
        boolean dataMasking = false;
        if (checkDataMasking && StringUtils.isNotBlank(tableName.getCatalog())) {
            dataMasking = columnNode.getName().hashCode() % 3 == 0;
            System.out.printf("dataMasking %s.%s.%s.%s = %s \n", tableName.getCatalog(),
                    tableName.getDb(), tableName.getTbl(), columnNode.getName(), dataMasking);
        }
        columnNode.setTableName(tableName.getTbl()).setDatabase(tableName.getDb()).setCatalog(tableName.getCatalog())
                .setDataMasking(dataMasking);
    }

    /**
     * @param exprs
     * @param columnAlias
     * @return
     */
    public static LinkedList<ColumnNode> buildColumnNodes(List<Expr> exprs, String columnAlias) {
        LinkedList<ColumnNode> columnNodes = new LinkedList<>();
        for (Expr expr : exprs) {
            buildColumnNodes(expr, columnAlias);
        }
        return columnNodes;
    }

    /**
     *
     * @param field
     * @param tableName
     * @param columnAlias
     * @param checkDataMasking
     * @return
     */
    public static ColumnNode buildColumnNode(Field field, TableName tableName, String columnAlias, boolean checkDataMasking) {
        ColumnNode columnNode = new ColumnNode().setName(field.getName()).setAlias(columnAlias);
        setTableName(columnNode, Objects.isNull(tableName) ? field.getRelationAlias() : tableName, checkDataMasking);
        return columnNode;
    }

    /**
     * @param columnNode
     * @param columnName
     * @return
     */
    public static boolean columnEquals(ColumnNode columnNode, String columnName) {
        return StringUtils.equals(columnName, columnNode.getName()) || StringUtils.equals(columnName, columnNode.getAlias());
    }

    /**
     * @param columnNode
     * @param tableName
     * @return
     */
    public static boolean matchFullTableName(ColumnNode columnNode, TableName tableName) {
        if (StringUtils.equals(columnNode.getTableFieldByType(TableFieldType.CATALOG_DB),
                getTableFieldByType(TableFieldType.CATALOG_DB, tableName))) {
            return StringUtils.equals(columnNode.getTableName(), getTableFieldByType(TableFieldType.TBL, tableName));
        }
        return false;
    }

    /**
     * @param tableFieldType
     * @param tableName
     * @return
     */
    public static String getTableFieldByType(TableFieldType tableFieldType, TableName tableName) {
        if (Objects.isNull(tableName))
            return null;
        switch (tableFieldType) {
            case CATALOG:
                return tableName.getCatalog();
            case DB:
                return tableName.getDb();
            case CATALOG_DB:
                return Joiner.on(".").skipNulls().join(tableName.getCatalog(), tableName.getDb());
            case CATALOG_DB_TBL:
                return Joiner.on(".").skipNulls().join(tableName.getCatalog(), tableName.getDb(), tableName);
            case DB_TBL:
                return Joiner.on(".").skipNulls().join(tableName.getDb(), tableName.getTbl());
            default:
                return tableName.getTbl();
        }
    }

    public static String getColumnName(Expr expr) {
        if (expr instanceof SlotRef) {
            return ((SlotRef) expr).getColumnName();
        }
        return null;
    }

    public static boolean matchFullTableName(ColumnNode columnNode, Relation relation) {
        if (Objects.isNull(relation)) return false;
        return StringUtils.equals(columnNode.getTableFieldByType(TableFieldType.CATALOG_DB_TBL),
                ColumnUtils.getTableFieldByType(TableFieldType.CATALOG_DB_TBL, relation.getAlias()));
    }

    public static boolean columnEquals(ColumnNode columnNode, Field field) {
        if (!columnEquals(columnNode, field.getName())) {
            return false;
        }
        TableName tableName = field.getRelationAlias();
        if (StringUtils.isNotBlank(columnNode.getCatalog())) {
            return matchFullTableName(columnNode, tableName);
        } else if (StringUtils.isNotBlank(columnNode.getDatabase())) {
            return StringUtils.equals(columnNode.getTableFieldByType(TableFieldType.DB_TBL)
                    , getTableFieldByType(TableFieldType.DB_TBL, tableName));

        } else if (StringUtils.isNotBlank(columnNode.getTableName())) {
            return StringUtils.equals(columnNode.getTableName(), getTableFieldByType(TableFieldType.TBL, tableName));
        }
        return true;
    }
}
