#include "blob_storage.h"
#include "blob_file_set.h"

namespace rocksdb
{
    namespace titandb
    {
        /**
         * Gets the blob record pointed by the blob index. The provided
         * buffer is used to store the record data, so the buffer must be
         * valid when the record is used.
         * 似乎所有的文件访问都先经过cache，通过index获取record，index存放在LSM中，首先通过index的file_number找到具体的文件，
         * 然后通过file_cache_ -> 获得对应的record
         * @param options
         * @param index filename + blobhandle
         * @param record 存放结果
         * @param buffer
         * @return
         */
        Status BlobStorage::Get(const ReadOptions &options, const BlobIndex &index,
                                BlobRecord *record, PinnableSlice *buffer)
        {
            // 这是因为BlobIndex可能过期？然后就需要先检查一下是否真的存在？
            auto sfile = FindFile(index.file_number).lock();
            if (!sfile)
                return Status::Corruption("Missing blob file: " +
                                          std::to_string(index.file_number));
            return file_cache_->Get(options, sfile->file_number(), sfile->file_size(),
                                    index.blob_handle, record, buffer);
        }
        /**
         * Creates a prefetcher for the specified file number.
         * 为某一个特定的file number 创建prefetcher
         * @param file_number
         * @param result
         * @return
         */
        Status BlobStorage::NewPrefetcher(uint64_t file_number,
                                          std::unique_ptr<BlobFilePrefetcher> *result)
        {
            // QUES: 以下
            // weak ptr 的lock方法，似乎是如果要访问weak ptr指向的对象的话就需要先进行lock操作，对其进行`提升`？
            auto sfile = FindFile(file_number).lock();
            if (!sfile)
                return Status::Corruption("Missing blob wfile: " +
                                          std::to_string(file_number));
            // 其实file_number就应该等于sfile->file_number()，那么实际上上面的FindFile操作只是为了获得file_size？
            // 因为有了file size才能够定位footer并进行读取，才能进行之后的读取操作
            return file_cache_->NewPrefetcher(sfile->file_number(), sfile->file_size(),
                                              result);
        }
        /**
         * 遍历BlobStorage中与ranges重叠的部分，然后将符合要求的文件放入files中，老version以及被丢弃的文件将不会放入
         * 需要注意的是ranges是有多个范围区间的
         * @param ranges
         * @param n 获得的文件的数目上限
         * @param include_end
         * @param files 保存位置
         * @return
         */
        Status BlobStorage::GetBlobFilesInRanges(const RangePtr *ranges, size_t n,
                                                 bool include_end,
                                                 std::vector<uint64_t> *files)
        {
            MutexLock l(&mutex_);
            for (size_t i = 0; i < n; i++)
            {
                const Slice *begin = ranges[i].start;
                const Slice *end = ranges[i].limit;
                // 一个column family 共用一个comparator
                auto cmp = cf_options_.comparator;

                std::string tmp;
                // nullptr means the minimum or maximum.
                for (auto it = ((begin != nullptr) ? blob_ranges_.lower_bound(*begin)
                                                   : blob_ranges_.begin());
                     it != ((end != nullptr) ? blob_ranges_.upper_bound(*end)
                                             : blob_ranges_.end());
                     it++)
                {
                    // Obsolete files are to be deleted, so just skip.
                    if (it->second->is_obsolete())
                        continue;
                    // 老的version的文件的largest_key以及smallest_key会被设置为空？
                    // The smallest and largest key of blob file meta of the old version are
                    // empty, so skip.
                    if (it->second->largest_key().empty() && end)
                        continue;

                    if ((end == nullptr) ||
                        (include_end && cmp->Compare(it->second->largest_key(), *end) <= 0) ||
                        (!include_end && cmp->Compare(it->second->largest_key(), *end) < 0))
                    {
                        files->push_back(it->second->file_number());
                        if (!tmp.empty())
                        {
                            tmp.append(" ");
                        }
                        tmp.append(std::to_string(it->second->file_number()));
                    }
                    assert(it->second->smallest_key().empty() ||
                           (!begin || cmp->Compare(it->second->smallest_key(), *begin) >= 0));
                }
                ROCKS_LOG_INFO(
                    db_options_.info_log,
                    "Get %" PRIuPTR " blob files [%s] in the range [%s, %s%c",
                    files->size(), tmp.c_str(), begin ? begin->ToString(true).c_str() : " ",
                    end ? end->ToString(true).c_str() : " ", include_end ? ']' : ')');
            }
            return Status::OK();
        }

        /**
         * Finds the blob file meta for the specified file number
         * 加锁，同时尝试在map类型的file_成员中寻找对应的文件，并返回文件所对应的blobFileMeta。
         * 需要注意的是这个和blob_file_cache的FindFile方法不同
         * @param file_number 文件编号
         * @return
         */
        std::weak_ptr<BlobFileMeta> BlobStorage::FindFile(uint64_t file_number) const
        {
            MutexLock l(&mutex_);
            auto it = files_.find(file_number);
            if (it != files_.end())
            {
                assert(file_number == it->second->file_number());
                return it->second;
            }
            return std::weak_ptr<BlobFileMeta>();
        }


        /**
         * Exports all blob files' meta. Only for tests.
         * 导出所有的 blob file 的meta数据，只是用来进行测试
         * @param ret
         */
        void BlobStorage::ExportBlobFiles(
            std::map<uint64_t, std::weak_ptr<BlobFileMeta>> &ret) const
        {
            ret.clear();
            MutexLock l(&mutex_);
            for (auto &kv : files_)
            {
                ret.emplace(kv.first, std::weak_ptr<BlobFileMeta>(kv.second));
            }
        }

        /**
         * 字面意思，需要修改files_, blob_ranges_, levels_file_count以及stats_
         * @param file
         */
        void BlobStorage::AddBlobFile(std::shared_ptr<BlobFileMeta> &file)
        {
            MutexLock l(&mutex_);
            files_.emplace(std::make_pair(file->file_number(), file));
            blob_ranges_.emplace(std::make_pair(Slice(file->smallest_key()), file));
            levels_file_count_[file->file_level()]++;
            // QUES: 下面应该是跟统计数据有关？
            if (file->live_data_size() != 0)
            {
                // When live data size == 0, it means the live size of blob file is unknown
                // now.
                // So don't count this metrics now, it will delayed to when setting the real
                // live data size
                // in `InitializeGC()` and `OnFlushCompleted()`/`OnCompactionCompleted()`.
                AddStats(stats_, cf_id_, file->GetDiscardableRatioLevel(), 1);
            }
            AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
                     file->file_size());
            AddStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
        }
        /**
         * Mark the file as obsolete, and retrun value indicates whether the file is founded.
         * @param file_number
         * @param obsolete_sequence
         * @return
         */
        bool BlobStorage::MarkFileObsolete(uint64_t file_number,
                                           SequenceNumber obsolete_sequence)
        {
            MutexLock l(&mutex_);
            auto file = files_.find(file_number);
            if (file == files_.end())
            {
                return false;
            }
            MarkFileObsoleteLocked(file->second, obsolete_sequence);
            return true;
        }

        /**
         * QUES: 还是不太懂这一套操作
         * 大概就是在obsolete_files_中添加文件编号以及过时编号，然后会转变文件的状态以及SubStats？
         * @param file
         * @param obsolete_sequence
         */
        void BlobStorage::MarkFileObsoleteLocked(std::shared_ptr<BlobFileMeta> file,
                                                 SequenceNumber obsolete_sequence)
        {
            mutex_.AssertHeld();

            obsolete_files_.push_back(
                std::make_pair(file->file_number(), obsolete_sequence));
            file->FileStateTransit(BlobFileMeta::FileEvent::kDelete);
            levels_file_count_[file->file_level()]--;
            SubStats(stats_, cf_id_, file->GetDiscardableRatioLevel(), 1);
            SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE,
                     file->live_data_size());
            SubStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_FILE_SIZE,
                     file->file_size());
            SubStats(stats_, cf_id_, TitanInternalStats::NUM_LIVE_BLOB_FILE, 1);
            AddStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
                     file->file_size());
            AddStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
        }

        /**
         * 在blob_ranges中去除文件，在files_中去除文件，在file_cache_中去除文件
         * @param file_number
         * @return
         */
        bool BlobStorage::RemoveFile(uint64_t file_number)
        {
            mutex_.AssertHeld();

            auto file = files_.find(file_number);
            if (file == files_.end())
            {
                return false;
            }
            // Removes from blob_ranges_
            // equal_range返回给定键的所有的值，一个开始迭代器，一个结束迭代器
            auto p = blob_ranges_.equal_range(file->second->smallest_key());
            for (auto it = p.first; it != p.second; it++)
            {
                if (it->second->file_number() == file->second->file_number())
                {
                    it = blob_ranges_.erase(it);
                    break;
                }
            }
            SubStats(stats_, cf_id_, TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
                     file->second->file_size());
            SubStats(stats_, cf_id_, TitanInternalStats::NUM_OBSOLETE_BLOB_FILE, 1);
            files_.erase(file_number);
            file_cache_->Evict(file_number);
            return true;
        }
        /**
         *
         * 将编号小于oldest_sequence的过时的文件存进行删除，如果obsolete_files不为null的话就在其中保存一份备份
         * @param obsolete_files
         * @param oldest_sequence
         */
        void BlobStorage::GetObsoleteFiles(std::vector<std::string> *obsolete_files,
                                           SequenceNumber oldest_sequence)
        {
            MutexLock l(&mutex_);

            for (auto it = obsolete_files_.begin(); it != obsolete_files_.end();)
            {
                auto &file_number = it->first;
                auto &obsolete_sequence = it->second;
                // We check whether the oldest snapshot is no less than the last sequence
                // by the time the blob file become obsolete. If so, the blob file is not
                // visible to all existing snapshots.
                if (oldest_sequence > obsolete_sequence)
                {
                    // remove obsolete files
                    bool __attribute__((__unused__)) removed = RemoveFile(file_number);
                    assert(removed);
                    ROCKS_LOG_INFO(db_options_.info_log,
                                   "Obsolete blob file %" PRIu64 " (obsolete at %" PRIu64
                                   ") not visible to oldest snapshot %" PRIu64 ", delete it.",
                                   file_number, obsolete_sequence, oldest_sequence);
                    if (obsolete_files)
                    {
                        obsolete_files->emplace_back(
                            BlobFileName(db_options_.dirname, file_number));
                    }

                    it = obsolete_files_.erase(it);
                    continue;
                }
                ++it;
            }
        }
        /**
         * 计算每一个文件的分数，然后根据分数进行排序，结果保存在gc_score中
         */
        void BlobStorage::ComputeGCScore()
        {
            // TODO: no need to recompute all everytime
            MutexLock l(&mutex_);
            gc_score_.clear();

            for (auto &file : files_)
            {
                if (file.second->is_obsolete())
                {
                    continue;
                }
                gc_score_.push_back({});
                auto &gcs = gc_score_.back();
                gcs.file_number = file.first;
                if (file.second->file_size() < cf_options_.merge_small_file_threshold)
                {
                    // for the small file or file with gc mark (usually the file that just
                    // recovered) we want gc these file but more hope to gc other file with
                    // more invalid data
                    gcs.score = cf_options_.blob_file_discardable_ratio;
                }
                else
                {
                    gcs.score = file.second->GetDiscardableRatio();
                }
            }

            std::sort(gc_score_.begin(), gc_score_.end(),
                      [](const GCScore &first, const GCScore &second) {
                          return first.score > second.score;
                      });
        }

    } // namespace titandb
} // namespace rocksdb
