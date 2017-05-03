package com.pinterest.secor.util;

import com.pinterest.secor.common.SecorConfig;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetUtil {
    public static int getParquetBlockSize(SecorConfig config) {
        return config.getInt("parquet.block.size", ParquetWriter.DEFAULT_BLOCK_SIZE);
    }

    public static int getParquetPageSize(SecorConfig config) {
        return config.getInt("parquet.page.size", ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    public static boolean getParquetEnableDictionary(SecorConfig config) {
        return config.getBoolean("parquet.enable.dictionary", ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED);
    }

    public static boolean getParquetValidation(SecorConfig config) {
        return config.getBoolean("parquet.validation", ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);
    }
}
