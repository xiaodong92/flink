package com.study.flink.utils

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

    private val DAY_DEFAULT_FORMAT = "yyyyMMdd"

    def getTodayDefault: String = {
        getToday(DAY_DEFAULT_FORMAT)
    }

    private def getToday(format: String): String = {
        FastDateFormat.getInstance(format).format(System.currentTimeMillis())
    }

}
