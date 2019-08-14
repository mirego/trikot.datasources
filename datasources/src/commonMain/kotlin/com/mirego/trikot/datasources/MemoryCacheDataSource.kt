package com.mirego.trikot.datasources

import com.mirego.trikot.streams.cancellable.CancellableManager
import com.mirego.trikot.streams.concurrent.AtomicReference
import com.mirego.trikot.streams.reactive.executable.BaseExecutablePublisher
import com.mirego.trikot.streams.reactive.executable.ExecutablePublisher

class MemoryCacheDataSource<R : DataSourceRequest, T> : BaseDataSource<R, T>() {
    private val memoryCache = AtomicReference<Map<Any, T?>>(HashMap())

    override fun internalRead(request: R): ExecutablePublisher<T> {
        return object : BaseExecutablePublisher<T>() {
            override fun internalRun(cancellableManager: CancellableManager) {
                memoryCache.value[request.cachableId]?.let {
                    dispatchSuccess(it)
                } ?: run {
                    dispatchError(NoSuchElementException())
                }
            }
        }
    }

    override fun save(request: R, data: T?) {
        val initialValue = memoryCache.value
        val mutableMap = initialValue.toMutableMap()
        mutableMap[request.cachableId] = data
        if (!memoryCache.compareAndSet(initialValue, mutableMap.toMap())) {
            save(request, data)
        } else {
            refreshPublisherWithId(request.cachableId)
        }
    }
}
