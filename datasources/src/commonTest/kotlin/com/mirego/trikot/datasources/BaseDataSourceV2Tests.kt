package com.mirego.trikot.datasources

import com.mirego.trikot.datasources.testutils.assertEquals
import com.mirego.trikot.streams.reactive.BehaviorSubject
import com.mirego.trikot.streams.reactive.Publishers
import com.mirego.trikot.streams.reactive.promise.Promise
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class BaseDataSourceV2Tests {

    private val requestUseCache = TestDataSourceRequest("1", DataSourceRequest.Type.USE_CACHE, DataSourceTestData("1"))
    private val requestRefreshCache = TestDataSourceRequest("1", DataSourceRequest.Type.REFRESH_CACHE, DataSourceTestData("1"))
    private val requestUseCache2 = TestDataSourceRequest("2", DataSourceRequest.Type.USE_CACHE, DataSourceTestData("2"))

    @Test
    fun whenCacheHasDataThenReadReturnsCacheDataAndInternalReadNotCalled() {
        val initialData = DataSourceTestData("value")
        val cacheDataSource = CacheDataSource(Promise.resolve(initialData))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)

        mainDataSource.read(requestUseCache).assertEquals(DataState.data(initialData))
        assertEquals(0, mainDataSource.internalReadCount)
    }

    @Test
    fun whenCacheIsValidThenReadWith_REFRESH_CACHE_ReturnsPendingWithDataAndInternalReadCalled() {
        val initialData = DataSourceTestData("value")
        val cacheDataSource = CacheDataSource(Promise.resolve(initialData))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)
        mainDataSource.read(requestRefreshCache).assertEquals(DataState.pending(initialData))
        assertEquals(1, mainDataSource.internalReadCount)
    }

    @Test
    fun givenPendingCacheDataWhenCacheDataCompletedThenReturningPendingWithCacheDataAndInternalReadCalled() {
        val cacheReadPublisher: BehaviorSubject<DataSourceTestData> = Publishers.behaviorSubject()
        val cacheDataSource = CacheDataSource(Promise.from(cacheReadPublisher))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)

        val publisher = mainDataSource.read(requestRefreshCache)
        publisher.assertEquals(DataState.pending())
        val cacheData = DataSourceTestData("value")
        cacheReadPublisher.value = cacheData
        publisher.assertEquals(DataState.pending(cacheData))
        assertEquals(1, mainDataSource.internalReadCount)
    }

    @Test
    fun givenPendingCacheDataWhenReadWithRefreshAndCacheDataCompletedAndDataIsAvailableThenReturningTheNewDataAndSaveCalled() {
        val cacheReadPublisher: BehaviorSubject<DataSourceTestData> = Publishers.behaviorSubject()
        val cacheDataSource = CacheDataSource(Promise.from(cacheReadPublisher))
        val newData = DataSourceTestData("newValue")
        val mainDataSource = MainDataSource(Promise.resolve(newData), cacheDataSource)

        val publisher = mainDataSource.read(requestRefreshCache)
        publisher.assertEquals(DataState.pending())
        val cacheData = DataSourceTestData("value")
        cacheReadPublisher.value = cacheData
        publisher.assertEquals(DataState.data(newData))
        assertEquals(1, mainDataSource.internalReadCount)
        assertEquals(1, cacheDataSource.internalSaveCount)
    }

    @Test
    fun when3ReadAtTheSameTimeThenInternalReadCalledOnlyOnce() {
        val initialData = DataSourceTestData("value")
        val cacheDataSource = CacheDataSource(Promise.resolve(initialData))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)

        mainDataSource.read(requestRefreshCache).assertEquals(DataState.pending(initialData))
        mainDataSource.read(requestRefreshCache).assertEquals(DataState.pending(initialData))
        mainDataSource.read(requestRefreshCache).assertEquals(DataState.pending(initialData))
        assertEquals(1, mainDataSource.internalReadCount)
    }

    @Test
    fun givenCacheDataWhen2ReadAreMadeThenSamePublisherIsReturned() {
        val initialData = DataSourceTestData("value")
        val cacheDataSource = CacheDataSource(Promise.resolve(initialData))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)

        val publisher1 = mainDataSource.read(requestRefreshCache)
        val publisher2 = mainDataSource.read(requestRefreshCache)
        assertSame(publisher1, publisher2)
    }

    @Test
    fun givenCachedDataWhenRefreshingWithoutAnySubscriberThenNextSubscriberReceiveRefreshedData() {
        val initialData = DataSourceTestData("value")
        val cacheDataSource = CacheDataSource(Promise.resolve(initialData))
        val mainDataSource = MainDataSource(Promise.from(Publishers.behaviorSubject()), cacheDataSource)

        mainDataSource.read(requestUseCache)
        val refreshData = DataSourceTestData("refreshValue")
        mainDataSource.readPromise = Promise.resolve(refreshData)
        mainDataSource.read(requestRefreshCache)
        mainDataSource.read(requestUseCache).assertEquals(DataState.data(refreshData))
    }

    @Test
    fun whenRefreshingWithCachedDataWhenAnErrorOccursThenCachedDataIsReturnedWithAnError() {
        val data = DataSourceTestData("data")
        val mainDataSource = MainDataSource(Promise.resolve(data))

        mainDataSource.read(requestUseCache).assertEquals(DataState.data(data))
        val error = Throwable()
        mainDataSource.readPromise = Promise.reject(error)
        mainDataSource.read(requestRefreshCache).assertEquals(DataState.error(error, data))
    }

    @Test
    fun whenNoCacheDataSourceThenStartInPendingThenDataReceived() {
        val readPublisher: BehaviorSubject<DataSourceTestData> = Publishers.behaviorSubject()
        val mainDataSource = MainDataSource(Promise.from(readPublisher))

        val publisher = mainDataSource.read(requestRefreshCache)
        publisher.assertEquals(DataState.pending())
        val data = DataSourceTestData("data")
        readPublisher.value = data
        publisher.assertEquals(DataState.data(data))
    }

    @Test
    fun when2ReadFromDifferentCacheIdThenDataIsDistinctAndInternalReadCalledTwice() {
        val mainDataSource = MainDataSource(Promise.reject(Throwable()), userRequestValue = true)

        mainDataSource.read(requestUseCache).assertEquals(DataState.data(requestUseCache.value))
        mainDataSource.read(requestUseCache2).assertEquals(DataState.data(requestUseCache2.value))
        assertEquals(2, mainDataSource.internalReadCount)
    }

    data class DataSourceTestData(
        val value: String
    )

    data class TestDataSourceRequest(
        override val cacheableId: Any,
        override val requestType: DataSourceRequest.Type,
        val value: DataSourceTestData
    ) : DataSourceRequest

    class MainDataSource(
        var readPromise: Promise<DataSourceTestData>,
        cacheDataSource: DataSource<TestDataSourceRequest, DataSourceTestData>? = null,
        private val userRequestValue: Boolean = false
    ) :
        BaseDataSourceV2<TestDataSourceRequest, DataSourceTestData>(cacheDataSource) {

        var internalReadCount = 0

        override fun internalRead(request: TestDataSourceRequest): Promise<DataSourceTestData> {
            internalReadCount++
            return if (userRequestValue) {
                Promise.resolve(request.value)
            } else {
                readPromise
            }
        }

        override fun delete(cacheableId: Any) {
        }
    }

    class CacheDataSource(private val readPromise: Promise<DataSourceTestData>) : BaseDataSourceV2<TestDataSourceRequest, DataSourceTestData>() {
        var internalSaveCount = 0

        override fun internalRead(request: TestDataSourceRequest): Promise<DataSourceTestData> {
            return readPromise
        }

        override fun save(request: TestDataSourceRequest, data: DataSourceTestData?) {
            internalSaveCount++
        }

        override fun delete(cacheableId: Any) {
        }
    }
}
