package com.starrocks.sql.analyzer.masking;

import com.alibaba.fastjson.JSON;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @ClassName DataMaskingAnalyzer
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 上午10:56
 */
public class DataMaskingAnalyzer {

    public void analyzeStatement(QueryStatement stmt) {
        System.out.println(stmt.getOrigStmt().originStmt);
        QueryRelation queryRelation = stmt.getQueryRelation();
        ColumnNode root = new ColumnNode();
        visitRelation(queryRelation, root);
        System.out.println(JSON.toJSONString(root));
    }

    protected void visitRelation(Relation relation, ColumnNode parent) {
        if (relation instanceof SelectRelation) {
            handleSelectRelation((SelectRelation)relation, parent);
        } else if (relation instanceof SubqueryRelation) {
            handleSubQueryRelation((SubqueryRelation)relation, parent);
        } else if (relation instanceof CTERelation) {
            handleCTERelation((CTERelation)relation, parent);
        } else if (relation instanceof JoinRelation) {
            handleJoinRelation((JoinRelation)relation, parent);
        } else if (relation instanceof ViewRelation) {
            handleViewRelation((ViewRelation)relation, parent);
        } else if (relation instanceof UnionRelation) {
            handleUnionRelation((UnionRelation)relation, parent);
        } else if (relation instanceof TableRelation) {
            handleTableRelation((TableRelation)relation, parent);
        }
    }

    private void handleTableRelation(TableRelation tableRelation, ColumnNode parent) {
        Map<Field, Column> columns = tableRelation.getColumns();
        for (Map.Entry<Field, Column> entry : columns.entrySet()) {
            Field field = entry.getKey();
            if (ColumnUtils.columnEquals(parent, field)) continue;
            ColumnNode columnNode = ColumnUtils.buildColumnNode(field, tableRelation.getName(), null, true);
            parent.addColumnNode(columnNode);
        }
        parent.refreshDataMasking();
    }

    private void handleUnionRelation(UnionRelation unionRelation, ColumnNode parent) {
        List<QueryRelation> relations = unionRelation.getRelations();
        if (StringUtils.isNotBlank(parent.getName())) {
            for (QueryRelation queryRelation : relations) {
                visitRelation(queryRelation, parent);
            }
        }else {
            List<String> columnOutputNames = unionRelation.getColumnOutputNames();
            String tableName = ColumnUtils.getTableFieldByType(TableFieldType.TBL, unionRelation.getAlias());
            for (String columnName: columnOutputNames) {
                ColumnNode columnNode = new ColumnNode().setName(columnName).setTableName(tableName);
                parent.addColumnNode(columnNode);
                for (QueryRelation queryRelation: relations) {
                    visitRelation(queryRelation, parent);
                }
            }
        }
        parent.refreshDataMasking();
    }

    private void handleViewRelation(ViewRelation viewRelation, ColumnNode parent) {
    }

    private void handleJoinRelation(JoinRelation joinRelation, ColumnNode parent) {
        if (ColumnUtils.matchFullTableName(parent, joinRelation.getLeft())) {
            visitRelation(joinRelation.getLeft(), parent);
        } else {
            if (joinRelation.getLeft() instanceof JoinRelation) {
                handleJoinRelation((JoinRelation) joinRelation.getLeft(), parent);
            }
        }

        if (ColumnUtils.matchFullTableName(parent, joinRelation.getRight())) {
            visitRelation(joinRelation.getRight(), parent);
        }
    }

    private void handleCTERelation(CTERelation cteRelation, ColumnNode parent) {
        visitRelation(cteRelation.getCteQueryStatement().getQueryRelation(), parent);
    }

    private void handleSubQueryRelation(SubqueryRelation subqueryRelation, ColumnNode parent) {
        ColumnUtils.setTableName(parent, subqueryRelation.getAlias(), false);
        visitRelation(subqueryRelation, parent);
    }

    /**
     *
     * @param selectRelation
     * @param parent
     */
    private void handleSelectRelation(SelectRelation selectRelation, ColumnNode parent) {
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
        Relation subRelation = selectRelation.getRelation();
        if (isSelectItemsEmpty(items)) {
            visitRelation(subRelation, parent);
        }else {
            handleSelectItems(subRelation, items, parent);
        }
        parent.refreshDataMasking();
    }

    /**
     *
     * @param subRelation
     * @param items
     * @param parent
     */
    private void handleSelectItems(Relation subRelation, List<SelectListItem> items, ColumnNode parent) {
        if (StringUtils.isBlank(parent.getName())) {
            for (SelectListItem item : items) {
                Expr expr = item.getExpr();
                LinkedList<ColumnNode> columnNodes = ColumnUtils.buildColumnNodes(expr, item.getAlias());
                parent.addColumnNodes(columnNodes);
                visitChildren(subRelation, columnNodes);
            }
        }else {
            SelectListItem item = getSelectItem(items, parent);
            if (Objects.isNull(item)) {
                visitRelation(subRelation, parent);
            } else {
                LinkedList<ColumnNode> columnNodes = ColumnUtils.buildColumnNodes(item.getExpr(), item.getAlias());
                parent.addColumnNodes(columnNodes);
                visitChildren(subRelation, columnNodes);
            }
        }
    }

    /**
     *
     * @param items
     * @param parent
     * @return
     */
    private SelectListItem getSelectItem(List<SelectListItem> items, ColumnNode parent) {
        for (SelectListItem item : items) {
            if (StringUtils.equals(item.getAlias(), parent.getName())) {
                return item;
            } else if (StringUtils.equals(ColumnUtils.getColumnName(item.getExpr()), parent.getName())) {
                return item;
            }
        }
        return null;
    }

    private void visitChildren(Relation subRelation, LinkedList<ColumnNode> columnNodes) {
        for (ColumnNode columnNode : columnNodes) {
            visitRelation(subRelation, columnNode);
        }
    }

    /**
     *
     * @param items
     * @return
     */
    private boolean isSelectItemsEmpty(List<SelectListItem> items) {
        return CollectionUtils.isEmpty(items) || Objects.isNull(items.get(0).getExpr());
    }
}
