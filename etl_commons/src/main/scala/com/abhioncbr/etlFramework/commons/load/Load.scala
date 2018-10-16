package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.file.FilePath

case class Load(subTask: String,
                loadType: LoadType.valueType,
                dbName: String,
                tableName: String,
                datasetName: String,
                feedName: String,
                dataPath: FilePath, //fileInitialPath: String,
                fileType: String,
                partData: PartitioningData )
