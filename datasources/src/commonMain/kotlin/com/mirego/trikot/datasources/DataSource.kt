package com.mirego.trikot.datasources

import com.mirego.trikot.foundation.CommonJSExport
import org.reactivestreams.Publisher

@CommonJSExport
interface DataSource<R : DataSourceRequest, T> {
    /**
     * Send a read request to the Datasource
     */
    fun read(request: R): Publisher<DataState<T, Throwable>>

    /**
     * Save data to the datasource
     */
    fun save(request: R, data: T?)
}
