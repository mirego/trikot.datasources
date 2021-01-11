package com.mirego.trikot.datasources

import com.mirego.trikot.streams.reactive.RefreshablePublisher

abstract class BaseDataStateDataSource<R : DataSourceRequest, T>(cacheDataSource: DataSource<R, T>? = null) :
    BaseDataSource<R, T>(cacheDataSource) {
    override fun createRefreshablePublisher(request: R): RefreshablePublisher<DataState<T, Throwable>> =
        DataStateRefreshablePublisher({ cancellableManager, isRefreshing ->
            readData(request, cancellableManager, isRefreshing)
        }, DataState.pending())
}
