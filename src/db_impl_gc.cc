#include "db_impl.h"

#include "test_util/sync_point.h"

#include "blob_file_iterator.h"
#include "blob_file_size_collector.h"
#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "util.h"

namespace rocksdb {
    namespace titandb {

        Status TitanDBImpl::ExtractGCStatsFromTableProperty(
                const std::shared_ptr<const TableProperties> &table_properties, bool to_add,
                std::map<uint64_t, int64_t> *blob_file_size_diff) {
            assert(blob_file_size_diff != nullptr);
            if (table_properties == nullptr) {
                // No table property found. File may not contain blob indices.
                return Status::OK();
            }
            return ExtractGCStatsFromTableProperty(*table_properties.get(), to_add,
                                                   blob_file_size_diff);
        }

        // 整个titan的to_add字段只有true，所以可以简单理解为就是true
        // 提取出table_properties的user_collected_properties，并保存其中的数据（`SST中有多少数据保存在Blob文件中`）
        Status TitanDBImpl::ExtractGCStatsFromTableProperty(
                const TableProperties &table_properties, bool to_add,
                std::map<uint64_t, int64_t> *blob_file_size_diff) {
            assert(blob_file_size_diff != nullptr);
            auto &prop = table_properties.user_collected_properties;
            auto prop_iter = prop.find(BlobFileSizeCollector::kPropertiesName);
            if (prop_iter == prop.end()) {
                // No table property found. File may not contain blob indices.
                return Status::OK();
            }
            Slice prop_slice(prop_iter->second);
            std::map<uint64_t, uint64_t> blob_file_sizes; // `blob文件编号` 到 `SST中有多少数据保存在Blob文件中`
            if (!BlobFileSizeCollector::Decode(&prop_slice, &blob_file_sizes)) {
                return Status::Corruption("Failed to decode blob file size property.");
            }
            for (const auto &blob_file_size : blob_file_sizes) {
                uint64_t file_number = blob_file_size.first;
                int64_t diff = static_cast<int64_t>(blob_file_size.second);
                if (!to_add) {
                    diff = -diff;
                }
                // QUES: 不懂为啥这个地方是使用的+=，难道一个file_number可能对应多个值吗
                // ANSW: 并不能，但是因为这个函数会被多次调用，
                (*blob_file_size_diff)[file_number] += diff;
            }
            return Status::OK();
        }

        // 1. 计算每一个cf对应的sst文件的property并得到对应每一个blob文件的数据大小，类似于MapReduce
        // 2. 重新计算每一个文件的gc score
        // 3. 将所有cf加入gc队列，并调用MaybeScheduleGC
        Status TitanDBImpl::InitializeGC(
                const std::vector<ColumnFamilyHandle *> &cf_handles) {
            assert(!initialized());
            Status s;
            FlushOptions flush_opts;
            flush_opts.wait = true;
            for (ColumnFamilyHandle *cf_handle : cf_handles) {
                // Flush memtable to make sure keys written by GC are all in SSTs.
                s = Flush(flush_opts, cf_handle);
                if (!s.ok()) {
                    return s;
                }
                TablePropertiesCollection collection;
                // 读取sst文件的property block从而获取其中的基本信息
                s = GetPropertiesOfAllTables(cf_handle, &collection);
                if (!s.ok()) {
                    return s;
                }
                // 通过对同一个diff多次调用来计算所有文件的数据
                // 因为一个SST文件可能会涉及到多个blob文件
                std::map<uint64_t, int64_t> blob_file_size_diff;
                for (auto &file : collection) {
                    // file.second就是这个SST文件对应的properties
                    s = ExtractGCStatsFromTableProperty(file.second, true /*to_add*/,
                                                        &blob_file_size_diff);
                    if (!s.ok()) {
                        return s;
                    }
                }
                // 根据cf_id获取blob_storage
                std::shared_ptr<BlobStorage> blob_storage =
                        blob_file_set_->GetBlobStorage(cf_handle->GetID()).lock();
                assert(blob_storage != nullptr);
                for (auto &file_size : blob_file_size_diff) {
                    assert(file_size.second >= 0);
                    std::shared_ptr<BlobFileMeta> file =
                            blob_storage->FindFile(file_size.first).lock();
                    if (file != nullptr) {
                        // 初始化的时候live_data_size应该是0？
                        assert(file->live_data_size() == 0);
                        // 设置live_data_size为刚刚计算出来的值
                        file->set_live_data_size(static_cast<uint64_t>(file_size.second));
                        // QUES: 不懂这个地方关于stat的操作
                        AddStats(stats_.get(), cf_handle->GetID(),
                                 file->GetDiscardableRatioLevel(), 1);
                    }
                }
                // 将所有的blob文件设置为kRestart状态
                // 其中会调用ComputeGCScore函数，用于计算gc分数
                blob_storage->InitializeAllFiles();
            }
            {
                MutexLock l(&mutex_);
                for (ColumnFamilyHandle *cf_handle : cf_handles) {
                    AddToGCQueue(cf_handle->GetID());
                }
                MaybeScheduleGC();
            }
            return s;
        }

        void TitanDBImpl::MaybeScheduleGC() {
            mutex_.AssertHeld();

            if (db_options_.disable_background_gc) return;

            if (shuting_down_.load(std::memory_order_acquire)) return;

            while (unscheduled_gc_ > 0 &&
                   bg_gc_scheduled_ < db_options_.max_background_gc) {
                unscheduled_gc_--;
                bg_gc_scheduled_++;
                thread_pool_->SubmitJob(std::bind(&TitanDBImpl::BGWorkGC, this));
            }
        }

        void TitanDBImpl::BGWorkGC(void *db) {
            reinterpret_cast<TitanDBImpl *>(db)->BackgroundCallGC();
        }
        // 对于每一个gc_queue中的cf，调用`BackgroundGC`函数，并做好日志记录
        void TitanDBImpl::BackgroundCallGC() {
            TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeGCRunning");
            {
                MutexLock l(&mutex_);
                assert(bg_gc_scheduled_ > 0);
                // 如果有删除cf的请求，那么就暂停线程的执行
                // QUES: 如果另一个线程已经进入到了函数下面会发生什么？也许在drop_cf函数中也有类似的东西使得两者不会交叉
                while (drop_cf_requests_ > 0) {
                    bg_cv_.Wait();
                }
                bg_gc_running_++;

                TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeBackgroundGC");
                if (!gc_queue_.empty()) {
                    uint32_t column_family_id = PopFirstFromGCQueue();
                    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                                         db_options_.info_log.get());
                    // 开始gc
                    BackgroundGC(&log_buffer, column_family_id);
                    // 应该是不让不同的gc线程的日志发生交叉？
                    {
                        mutex_.Unlock();
                        log_buffer.FlushBufferToLog();
                        LogFlush(db_options_.info_log.get());
                        mutex_.Lock();
                    }
                }

                bg_gc_running_--;
                bg_gc_scheduled_--;
                // 因为可能是因为到达了设定的gc线程的上限才没有执行所有的任务，可能还是会有unscheduled_gc_
                // 所以需要再次调用
                MaybeScheduleGC();
                if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
                    // Signal DB destructor if bg_gc_scheduled_ drop to 0.
                    // Signal drop CF requests if bg_gc_running_ drop to 0.
                    // If none of this is true, there is no need to signal since nobody is
                    // waiting for it.
                    bg_cv_.SignalAll();
                }
                // IMPORTANT: there should be no code after calling SignalAll. This call may
                // signal the DB destructor that it's OK to proceed with destruction. In
                // that case, all DB variables will be deallocated and referencing them
                // will cause trouble.
            }
        }

        Status TitanDBImpl::BackgroundGC(LogBuffer *log_buffer,
                                         uint32_t column_family_id) {
            mutex_.AssertHeld();

            std::unique_ptr<BlobGC> blob_gc;
            bool gc_merge_rewrite = false;
            std::unique_ptr<ColumnFamilyHandle> cfh;
            Status s;

            std::shared_ptr<BlobStorage> blob_storage;
            // Skip CFs that have been dropped.
            if (!blob_file_set_->IsColumnFamilyObsolete(column_family_id)) {
                // 从blob_file_set中获取cf_id对应的blob_storage
                blob_storage = blob_file_set_->GetBlobStorage(column_family_id).lock();
            } else {
                TEST_SYNC_POINT_CALLBACK("TitanDBImpl::BackgroundGC:CFDropped", nullptr);
                ROCKS_LOG_BUFFER(log_buffer, "GC skip dropped colum family [%s].",
                                 cf_info_[column_family_id].name.c_str());
            }
            if (blob_storage != nullptr) {
                const auto &cf_options = blob_storage->cf_options();
                std::shared_ptr<BlobGCPicker> blob_gc_picker =
                        std::make_shared<BasicBlobGCPicker>(db_options_, cf_options,
                                                            stats_.get());
                blob_gc = blob_gc_picker->PickBlobGC(blob_storage.get());

                if (blob_gc) {
                    cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
                    assert(column_family_id == cfh->GetID());
                    // 需要单独设置cf handle
                    blob_gc->SetColumnFamily(cfh.get());
                    // 从cf的配置中找到是否使用gc_merge_rewrite
                    gc_merge_rewrite =
                            cf_info_[column_family_id].mutable_cf_options.gc_merge_rewrite;
                }
            }

            // TODO(@DorianZheng) Make sure enough room for GC

            if (UNLIKELY(!blob_gc)) {
                RecordTick(statistics(stats_.get()), TITAN_GC_NO_NEED, 1);
                // Nothing to do
                ROCKS_LOG_BUFFER(log_buffer, "Titan GC nothing to do");
            } else {
                StopWatch gc_sw(env_, statistics(stats_.get()), TITAN_GC_MICROS);
                BlobGCJob blob_gc_job(blob_gc.get(), db_, &mutex_, db_options_,
                                      gc_merge_rewrite, env_, env_options_,
                                      blob_manager_.get(), blob_file_set_.get(), log_buffer,
                                      &shuting_down_, stats_.get());
                s = blob_gc_job.Prepare();
                if (s.ok()) {
                    mutex_.Unlock();
                    TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::BeforeRunGCJob");
                    s = blob_gc_job.Run();
                    TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::AfterRunGCJob");
                    mutex_.Lock();
                }
                if (s.ok()) {
                    s = blob_gc_job.Finish();
                }
                blob_gc->ReleaseGcFiles();
                // 如果cf还需要进行下一轮的gc，并且正在进行的gc任务和还没有完成的任务的和小于设定的最大值*2，
                // 那么就将cf再次加入gc队列中
                if (blob_gc->trigger_next() &&
                    (bg_gc_scheduled_ - 1 + gc_queue_.size() <
                     2 * static_cast<uint32_t>(db_options_.max_background_gc))) {
                    RecordTick(statistics(stats_.get()), TITAN_GC_TRIGGER_NEXT, 1);
                    // There is still data remained to be GCed
                    // and the queue is not overwhelmed
                    // then put this cf to GC queue for next GC
                    AddToGCQueue(blob_gc->column_family_handle()->GetID());
                }
            }

            if (s.ok()) {
                RecordTick(statistics(stats_.get()), TITAN_GC_SUCCESS, 1);
                // Done
            } else {
                SetBGError(s);
                RecordTick(statistics(stats_.get()), TITAN_GC_FAILURE, 1);
                ROCKS_LOG_WARN(db_options_.info_log, "Titan GC error: %s",
                               s.ToString().c_str());
            }

            TEST_SYNC_POINT("TitanDBImpl::BackgroundGC:Finish");
            return s;
        }

        Status TitanDBImpl::TEST_StartGC(uint32_t column_family_id) {
            // BackgroundCallGC
            Status s;
            LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
            {
                MutexLock l(&mutex_);
                // Prevent CF being dropped while GC is running.
                while (drop_cf_requests_ > 0) {
                    bg_cv_.Wait();
                }
                bg_gc_running_++;
                bg_gc_scheduled_++;

                s = BackgroundGC(&log_buffer, column_family_id);

                {
                    mutex_.Unlock();
                    log_buffer.FlushBufferToLog();
                    LogFlush(db_options_.info_log.get());
                    mutex_.Lock();
                }

                bg_gc_running_--;
                bg_gc_scheduled_--;
                if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
                    bg_cv_.SignalAll();
                }
            }
            return s;
        }

        void TitanDBImpl::TEST_WaitForBackgroundGC() {
            MutexLock l(&mutex_);
            while (bg_gc_scheduled_ > 0) {
                bg_cv_.Wait();
            }
        }

    }  // namespace titandb
}  // namespace rocksdb
