// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer.masking;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ViewRelation;
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
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        QueryRelation queryRelation = stmt.getQueryRelation();
        ColumnNode root = new ColumnNode();
        visitRelation(queryRelation, root);
        System.out.printf("column size: %s, data masking is : %s%n",
                root.getChildren().size(), root.isDataMasking());
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println(JSON.toJSONString(root, SerializerFeature.PrettyFormat));
    }

    protected void visitRelation(Relation relation, ColumnNode parent) {
        if (relation instanceof SelectRelation) {
            handleSelectRelation((SelectRelation) relation, parent);
        } else if (relation instanceof SubqueryRelation) {
            handleSubQueryRelation((SubqueryRelation) relation, parent);
        } else if (relation instanceof CTERelation) {
            handleCTERelation((CTERelation) relation, parent);
        } else if (relation instanceof JoinRelation) {
            handleJoinRelation((JoinRelation) relation, parent);
        } else if (relation instanceof ViewRelation) {
            handleViewRelation((ViewRelation) relation, parent);
        } else if (relation instanceof UnionRelation) {
            handleUnionRelation((UnionRelation) relation, parent);
        } else if (relation instanceof TableRelation) {
            handleTableRelation((TableRelation) relation, parent);
        }
    }

    private void handleTableRelation(TableRelation tableRelation, ColumnNode parent) {
        Map<Field, Column> columns = tableRelation.getColumns();
        for (Map.Entry<Field, Column> entry : columns.entrySet()) {
            Field field = entry.getKey();
            if (!ColumnUtils.columnEquals(parent, field.getName())) {
                continue;
            }
            ColumnNode columnNode = ColumnUtils.buildColumnNode(field, tableRelation.getName(), null, true);
            parent.addColumnNode(columnNode);
        }
    }

    private void handleUnionRelation(UnionRelation unionRelation, ColumnNode parent) {
        List<QueryRelation> relations = unionRelation.getRelations();
        if (StringUtils.isNotBlank(parent.getName())) {
            for (QueryRelation queryRelation : relations) {
                visitRelation(queryRelation, parent);
            }
        } else {
            List<String> columnOutputNames = unionRelation.getColumnOutputNames();
            String tableName = ColumnUtils.getTableFieldByType(TableFieldType.TBL, unionRelation.getAlias());
            for (String columnName : columnOutputNames) {
                ColumnNode columnNode = new ColumnNode().setName(columnName).setTableName(tableName);
                parent.addColumnNode(columnNode);
                for (QueryRelation queryRelation : relations) {
                    visitRelation(queryRelation, parent);
                }
            }
        }
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
     * @param selectRelation
     * @param parent
     */
    private void handleSelectRelation(SelectRelation selectRelation, ColumnNode parent) {
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
        Relation subRelation = selectRelation.getRelation();
        if (isSelectItemsEmpty(items)) {
            visitRelation(subRelation, parent);
        } else {
            handleSelectItems(subRelation, items, parent);
        }
    }

    /**
     * @param subRelation
     * @param items
     * @param parent
     */
    private void handleSelectItems(Relation subRelation, List<SelectListItem> items, ColumnNode parent) {
        if (StringUtils.isBlank(parent.getName())) {
            for (SelectListItem item : items) {
                Expr expr = item.getExpr();
                LinkedList<ColumnNode> columnNodes = ColumnUtils.buildColumnNodes(expr, item.getAlias());
                addOneColumn(parent, columnNodes, StringUtils.isBlank(item.getAlias()) ? parent.getName() : item.getAlias());
                visitChildren(subRelation, columnNodes);
            }
        } else {
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

    private void addOneColumn(ColumnNode parent, LinkedList<ColumnNode> columnNodes, String columnName) {
        if (Objects.isNull(columnNodes) || CollectionUtils.isEmpty(columnNodes)) {
            parent.addColumnNode(new ColumnNode().setName(columnName));
            return;
        }
        if (columnNodes.size() == 1) {
            parent.addColumnNode(columnNodes.get(0));
        } else {
            ColumnNode columnNode = new ColumnNode().setName(columnName).setTableName(parent.getTableName());
            columnNode.addColumnNodes(columnNodes);
            parent.addColumnNode(columnNode);
        }

    }

    /**
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
     * @param items
     * @return
     */
    private boolean isSelectItemsEmpty(List<SelectListItem> items) {
        return CollectionUtils.isEmpty(items) || Objects.isNull(items.get(0).getExpr());
    }
}
