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

/**
 * @ClassName TableNameType
 * @Author guanxianchun
 * @Description
 * @Date 2023/11/5 上午11:59
 */
public enum TableFieldType {
    CATALOG,
    CATALOG_DB,
    CATALOG_DB_TBL,
    DB, DB_TBL,
    TBL,
    TBL_COLUMN,
    TBL_COLUMN_ALIAS
}
