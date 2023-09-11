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

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.PaimonTable;
import mockit.Mocked;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_KEY_STATS;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;

public class PaimonScanNodeTest {

    @Test
    public void testTotalFileLength(@Mocked PaimonTable table) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1).withDataFiles(meta1)
                .isStreaming(false).build();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        long totalFileLength = scanNode.getTotalFileLength(split);

        Assert.assertEquals(200, totalFileLength);
    }
}