package com.mirego.trikot.datasources

import com.mirego.trikot.datasources.extensions.data
import com.mirego.trikot.foundation.concurrent.AtomicReference
import com.mirego.trikot.streams.cancellable.CancellableManager
import com.mirego.trikot.streams.reactive.BehaviorSubject
import com.mirego.trikot.streams.reactive.Publishers
import com.mirego.trikot.streams.reactive.distinctUntilChanged
import com.mirego.trikot.streams.reactive.filter
import com.mirego.trikot.streams.reactive.filterNotNull
import com.mirego.trikot.streams.reactive.first
import com.mirego.trikot.streams.reactive.map
import com.mirego.trikot.streams.reactive.processors.combine
import com.mirego.trikot.streams.reactive.promise.Promise
import com.mirego.trikot.streams.reactive.shared
import com.mirego.trikot.streams.reactive.subscribe
import org.reactivestreams.Publisher

private typealias NullableDataState<T> = BaseDataSourceV2.NullableValue<DataState<T, Throwable>>
private typealias ReadPublisher<T> = Publisher<NullableDataState<T>>
private typealias DataPublisher<T> = BehaviorSubject<NullableDataState<T>>

abstract class BaseDataSourceV2<R : DataSourceRequest, T>(private val cacheDataSource: DataSource<R, T>? = null) : DataSource<R, T> {

    private val cacheIdToReadInProgress = AtomicReference<Map<Any, Boolean>>(HashMap())
    private val cacheIdToPublisher = AtomicReference<Map<Any, PublisherTriple<T>>>(HashMap())

    override fun read(request: R): Publisher<DataState<T, Throwable>> {
        val publishers = getPublishers(request)
        publishers.mergeDataPublisher.first().subscribe(CancellableManager(), onNext = {
            readIfNeeded(request, it, publishers.dataPublisher)
        })
        return publishers.sharedPublisher
    }

    private fun getPublishers(request: R): PublisherTriple<T> {
        val cacheableId = request.cacheableId
        val publisher = cacheIdToPublisher.value[cacheableId]
        return if (publisher != null) {
            publisher
        } else {
            val dataPublisher = Publishers.behaviorSubject<NullableDataState<T>>(NullableValue(null))
            val mergeDataPublisher = buildMergeDataPublisher(request, dataPublisher)
            val sharedPublisher = mergeDataPublisher
                .filterNotNull { it.value }
                .distinctUntilChanged()
                .shared()
            savePublisherToRegistry(PublisherTriple(sharedPublisher, mergeDataPublisher, dataPublisher), request)
        }
    }

    private fun savePublisherToRegistry(publisher: PublisherTriple<T>, request: R): PublisherTriple<T> {
        val initialMap = cacheIdToPublisher.value
        val mutableMap = initialMap.toMutableMap()
        mutableMap[request.cacheableId] = publisher
        return when {
            cacheIdToPublisher.compareAndSet(initialMap, mutableMap) -> publisher
            else -> getPublishers(request)
        }
    }

    private fun buildMergeDataPublisher(request: R, dataPublisher: DataPublisher<T>): ReadPublisher<T> {
        val cachePublisher = cacheDataSource?.read(request)
        return (cachePublisher?.safeCombine(dataPublisher)?.map { (cacheData: DataState<T, Throwable>, data: NullableDataState<T>) ->
            if (data.value != null) {
                data
            } else {
                val newValue = NullableValue(cacheData)
                if (!cacheData.isPending()) {
                    readIfNeeded(request, newValue, dataPublisher)
                }
                newValue
            }
        } ?: dataPublisher)
    }

    private fun readIfNeeded(request: R, currentData: NullableDataState<T>, dataPublisher: DataPublisher<T>) {
        if (shouldRead(request, currentData)) {
            doInternalRead(request, currentData, dataPublisher)
        }
    }

    private fun shouldRead(request: R, currentReadData: NullableDataState<T>): Boolean =
        currentReadData.value?.let { internalShouldRead(request, it) } ?: true

    open fun internalShouldRead(request: R, data: DataState<T, Throwable>): Boolean {
        return when (data) {
            is DataState.Pending -> false
            is DataState.Data -> request.requestType == DataSourceRequest.Type.REFRESH_CACHE
            is DataState.Error -> true
        }
    }

    private fun doInternalRead(request: R, currentData: NullableDataState<T>, dataPublisher: DataPublisher<T>) {
        if (!isReadInProgress(request)) {
            setReadInProgress(request, true)
            dataPublisher.value = NullableDataState(DataState.pending(currentData.value?.data()))
            internalRead(request)
                .onSuccess {
                    cacheDataSource?.save(request, it)
                    dataPublisher.value = NullableValue(DataState.data(it))
                }
                .onError {
                    dataPublisher.value = NullableValue(DataState.error(it, currentData.value?.data()))
                }
                .finally {
                    setReadInProgress(request, false)
                }
        }
    }

    private fun isReadInProgress(request: R) = cacheIdToReadInProgress.value[request.cacheableId] == true

    private fun setReadInProgress(request: R, inProgress: Boolean) {
        val initialMap = cacheIdToReadInProgress.value
        val mutableMap = initialMap.toMutableMap()
        mutableMap[request.cacheableId] = inProgress
        cacheIdToReadInProgress.compareAndSet(initialMap, mutableMap)
    }

    abstract fun internalRead(request: R): Promise<T>

    override fun save(request: R, data: T?) {}

    private data class PublisherTriple<T>(
        val sharedPublisher: Publisher<DataState<T, Throwable>>,
        val mergeDataPublisher: ReadPublisher<T>,
        val dataPublisher: DataPublisher<T>
    )

    data class NullableValue<T>(
        val value: T?
    )
}

@Suppress("UNCHECKED_CAST")
fun <T, R> Publisher<T>.safeCombine(publisher: Publisher<R>): Publisher<Pair<T, R>> {
    return (this as Publisher<Any>).combine(listOf(publisher) as List<Publisher<Any>>)
        .filter { list -> list[0] as? T != null && list[1] as? T != null }
        .map { list ->
            list[0] as T to list[1] as R
        }
}
