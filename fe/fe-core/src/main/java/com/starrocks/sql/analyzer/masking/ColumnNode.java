package com.starrocks.sql.analyzer.masking;

import com.google.common.base.Joiner;
import org.apache.commons.collections4.CollectionUtils;

import java.util.LinkedList;
import java.util.Objects;

/**
 * @ClassName ColumnNode
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 上午11:01
 */
public class ColumnNode {
    private String name;
    private String alias;
    private String tableName;
    private String database;
    private String catalog;
    private boolean dataMasking = false;
    private final LinkedList<ColumnNode> children = new LinkedList<>();

    public String getName() {
        return name;
    }

    public ColumnNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public ColumnNode setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public ColumnNode setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public ColumnNode setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public ColumnNode setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public LinkedList<ColumnNode> getChildren() {
        return children;
    }

    public boolean isDataMasking() {
        return dataMasking;
    }

    public ColumnNode setDataMasking(boolean dataMasking) {
        this.dataMasking = dataMasking;
        return this;
    }

    public void refreshDataMasking() {
        for (ColumnNode node : children) {
            if (node.isDataMasking()) {
                dataMasking = true;
                break;
            }
        }
    }

    public String getTableFieldByType(TableFieldType tableFieldType) {
        switch (tableFieldType){
            case CATALOG:
                return catalog;
            case DB:
                return database;
            case TBL:
                return tableName;
            case CATALOG_DB:
                return Joiner.on(".").skipNulls().join(catalog, database);
            case CATALOG_DB_TBL:
                return Joiner.on(".").skipNulls().join(catalog, database, tableName);
            case DB_TBL:
                return Joiner.on(".").skipNulls().join(database, tableName);
            case TBL_COLUMN:
                return Joiner.on(".").skipNulls().join(tableName, name);
            case TBL_COLUMN_ALIAS:
                return Joiner.on(".").skipNulls().join(tableName, alias);
            default:
                return name;
        }
    }

    public void addColumnNodes(LinkedList<ColumnNode> columnNodes) {
        if (CollectionUtils.isEmpty(columnNodes)) {
            return;
        }
        children.addAll(columnNodes);
//        refreshDataMasking();
    }

    public void addColumnNode(ColumnNode columnNode) {
        if (Objects.isNull(columnNode)) return;
        children.add(columnNode);
    }
}
