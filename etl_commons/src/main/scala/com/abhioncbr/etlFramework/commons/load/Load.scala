package com.abhioncbr.etlFramework.commons.load

case class Load(subTask: String,
                loadType: String,
                dbName: String,
                tableName: String,
                datasetName: String,
                feedName: String,
                fileInitialPath: String,
                fileType: String,
                partData: PartitioningData )
