/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.segment.index;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ColumnMetadataTest {
  private static Logger LOGGER = LoggerFactory.getLogger(ColumnMetadataTest.class);

  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(ColumnMetadataTest.class.toString());

  @BeforeClass
  public void setUP() throws Exception {

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final String filePath =
        TestUtils
            .getFileFromResourceUrl(ColumnMetadataTest.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.HOURS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testAllFieldsInitialized()
      throws Exception {
    final IndexSegmentImpl segment =
        (IndexSegmentImpl) Loaders.IndexSegment.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    ColumnMetadata col7Meta =  metadata.getColumnMetadataFor("column7");
    Assert.assertEquals("column7", col7Meta.getColumnName());
    Assert.assertEquals(359, col7Meta.getCardinality());
    Assert.assertEquals(false, col7Meta.isSingleValue());
    Assert.assertEquals(FieldSpec.DataType.INT, col7Meta.getDataType());
    Assert.assertEquals(9, col7Meta.getBitsPerElement());
    Assert.assertEquals(FieldSpec.FieldType.DIMENSION, col7Meta.getFieldType());
    Assert.assertEquals(24, col7Meta.getMaxNumberOfMultiValues());
    Assert.assertEquals(0, col7Meta.getTotalAggreateDocs());
    Assert.assertEquals(100000, col7Meta.getTotalRawDocs());
    Assert.assertEquals(100000, col7Meta.getTotalDocs());
    Assert.assertEquals(0, col7Meta.getStringColumnMaxLength());
    Assert.assertEquals(true, col7Meta.hasDictionary());
    Assert.assertEquals(true, col7Meta.isHasInvertedIndex());
    Assert.assertEquals(false, col7Meta.hasNulls());
    Assert.assertEquals(false, col7Meta.isSorted());
    // since this is MV field
    Assert.assertTrue(col7Meta.getTotalDocs() < col7Meta.getTotalNumberOfEntries());


    // single valued string field
    ColumnMetadata col3 = metadata.getColumnMetadataFor("column3");
    Assert.assertEquals(FieldSpec.DataType.STRING, col3.getDataType());
    Assert.assertEquals(true, col3.isSingleValue());
    Assert.assertEquals(4, col3.getStringColumnMaxLength());
    // this will be default
    Assert.assertEquals(0, col3.getMaxNumberOfMultiValues());

    ColumnMetadata timeColumn = metadata.getColumnMetadataFor("daysSinceEpoch");
    Assert.assertEquals(TimeUnit.HOURS, timeColumn.getTimeunit());
  }
}