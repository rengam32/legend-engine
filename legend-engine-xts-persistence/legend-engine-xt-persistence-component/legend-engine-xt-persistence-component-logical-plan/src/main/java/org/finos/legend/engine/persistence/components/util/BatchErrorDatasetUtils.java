package org.finos.legend.engine.persistence.components.util;

import org.finos.legend.engine.persistence.components.logicalplan.conditions.Condition;
import org.finos.legend.engine.persistence.components.logicalplan.conditions.Exists;
import org.finos.legend.engine.persistence.components.logicalplan.conditions.Not;
import org.finos.legend.engine.persistence.components.logicalplan.datasets.Dataset;
import org.finos.legend.engine.persistence.components.logicalplan.datasets.DatasetReference;
import org.finos.legend.engine.persistence.components.logicalplan.datasets.Selection;
import org.finos.legend.engine.persistence.components.logicalplan.operations.Insert;
import org.finos.legend.engine.persistence.components.logicalplan.values.*;

import java.util.Arrays;
import java.util.List;

public class BatchErrorDatasetUtils
{
    private final BatchErrorDataset batchErrorDataset;
    private final Dataset dataset;

    public BatchErrorDatasetUtils(BatchErrorDataset batchErrorDataset, Dataset dataset)
    {
        this.batchErrorDataset = batchErrorDataset;
        this.dataset = dataset;
    }

    public Insert insertBatchError(String requestId, String tableName, String errorMessage, String errorCategory, BatchStartTimestamp batchStartTimestamp)
    {
        DatasetReference metaTableRef = this.dataset.datasetReference();
        FieldValue idField = FieldValue.builder().datasetRef(metaTableRef).fieldName(batchErrorDataset.requestIdField()).build();
        FieldValue tableField = FieldValue.builder().datasetRef(metaTableRef).fieldName(batchErrorDataset.tableNameField()).build();
        FieldValue errorMessageField = FieldValue.builder().datasetRef(metaTableRef).fieldName(batchErrorDataset.errorMessageField()).build();
        FieldValue errorCategoryField = FieldValue.builder().datasetRef(metaTableRef).fieldName(batchErrorDataset.errorCategoryField()).build();
        FieldValue insertTimeField = FieldValue.builder().datasetRef(metaTableRef).fieldName(batchErrorDataset.createdOnField()).build();
        List<Value> insertFields = Arrays.asList(idField, tableField, errorMessageField, errorCategoryField, insertTimeField);
        List<Value> selectFields = Arrays.asList(StringValue.of(requestId), StringValue.of(tableName), StringValue.of(errorMessage), StringValue.of(errorCategory), batchStartTimestamp);
        return Insert.of(dataset, Selection.builder().addAllFields(selectFields).build(), insertFields);
    }
}
