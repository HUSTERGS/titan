#include "blob_file_set.h"

#include <inttypes.h>

#include "edit_collector.h"

namespace rocksdb
{
    namespace titandb
    {

        const size_t kMaxFileCacheSize = 1024 * 1024;

        BlobFileSet::BlobFileSet(const TitanDBOptions &options, TitanStats *stats)
            : dirname_(options.dirname),
              env_(options.env),
              env_options_(options),
              db_options_(options),
              stats_(stats)
        {
            auto file_cache_size = db_options_.max_open_files;
            if (file_cache_size < 0)
            {
                file_cache_size = kMaxFileCacheSize;
            }
            file_cache_ = NewLRUCache(file_cache_size);
        }

        Status BlobFileSet::Open(
            const std::map<uint32_t, TitanCFOptions> &column_families)
        {
            // Sets up initial column families.
            AddColumnFamilies(column_families);
            // 如果manifest文件已经存在的话，那么就进行恢复...？
            Status s = env_->FileExists(CurrentFileName(dirname_));
            if (s.ok())
            {
                return Recover();
            }
            if (!s.IsNotFound())
            {
                return s;
            }
            return OpenManifest(NewFileNumber());
        }
        // 
        Status BlobFileSet::Recover()
        {
            struct LogReporter : public log::Reader::Reporter
            {
                Status *status;
                void Corruption(size_t, const Status &s) override
                {
                    if (status->ok())
                        *status = s;
                }
            };

            // Reads "CURRENT" file, which contains the name of the current manifest file.
            std::string manifest;
            // 读取`CURRENY`文件，将其内容保存在manifest变量中，指示了当前manifets文件的文件名
            Status s = ReadFileToString(env_, CurrentFileName(dirname_), &manifest);
            if (!s.ok())
                return s;
            // 通过manifest文件是否以换行符为结尾来判断文件是否损坏
            if (manifest.empty() || manifest.back() != '\n')
            {
                return Status::Corruption("CURRENT file does not end with newline");
            }
            manifest.resize(manifest.size() - 1);

            // Opens the current manifest file.
            auto file_name = dirname_ + "/" + manifest;
            std::unique_ptr<SequentialFileReader> file;
            // 像这种突然就来一个大括号什么意思
            // 对于manifest文件的读取做了优化，具体来说是传入的option中将user_direct_read 设置为了false
            // 这一段可以理解为为了让局部变量自动消失...？
            {
                std::unique_ptr<SequentialFile> f;
                s = env_->NewSequentialFile(file_name, &f,
                                            env_->OptimizeForManifestRead(env_options_));
                if (!s.ok())
                    return s;
                file.reset(new SequentialFileReader(std::move(f), file_name));
            }

            // Reads edits from the manifest and applies them one by one.
            {
                LogReporter reporter;
                reporter.status = &s;
                log::Reader reader(nullptr, std::move(file), &reporter, true /*checksum*/,
                                   0 /*log_num*/);
                Slice record;
                std::string scratch;
                EditCollector collector;
                // 其实并不清楚manifest文件的构造...？
                while (reader.ReadRecord(&record, &scratch) && s.ok())
                {
                    VersionEdit edit;
                    s = DecodeInto(record, &edit);
                    if (!s.ok())
                        return s;
                    s = collector.AddEdit(edit);
                    if (!s.ok())
                        return s;
                }
                // 不断收集edit操作，整合之后统一进行apply操作
                s = collector.Seal(*this);
                if (!s.ok())
                    return s;
                s = collector.Apply(*this);
                if (!s.ok())
                    return s;

                uint64_t next_file_number = 0;
                s = collector.GetNextFileNumber(&next_file_number);
                if (!s.ok())
                    return s;
                next_file_number_.store(next_file_number);
                ROCKS_LOG_INFO(db_options_.info_log,
                               "Next blob file number is %" PRIu64 ".", next_file_number);
            }

            auto new_manifest_file_number = NewFileNumber();
            s = OpenManifest(new_manifest_file_number);
            if (!s.ok())
                return s;

            // Purge inactive files at start
            std::set<uint64_t> alive_files;
            alive_files.insert(new_manifest_file_number);
            for (const auto &bs : column_families_)
            {
                // 这里拼接得到的files_str似乎就只是为了下面的ROCKS_LOG_INFO进行输出
                std::string files_str;
                for (const auto &f : bs.second->files_)
                {
                    if (!files_str.empty())
                    {
                        files_str.append(", ");
                    }
                    files_str.append(std::to_string(f.first));
                    if (f.second->is_obsolete())
                    {
                        files_str.append("(obsolete)");
                    }
                }
                ROCKS_LOG_INFO(db_options_.info_log,
                               "Blob files for CF %" PRIu32 " found: %s", bs.first,
                               files_str.c_str());
                // delete obsoleted files at reopen
                // all the obsolete files's obsolete sequence are 0
                bs.second->GetObsoleteFiles(nullptr, kMaxSequenceNumber);
                // 记录所有的活动的文件
                for (const auto &f : bs.second->files_)
                {
                    alive_files.insert(f.second->file_number());
                }
            }
            std::vector<std::string> files;
            env_->GetChildren(dirname_, &files);
            // 删除不活动的文件，也就是不在alive_files中的文件
            for (const auto &f : files)
            {
                uint64_t file_number;
                FileType file_type;
                if (!ParseFileName(f, &file_number, &file_type))
                    continue;
                if (alive_files.find(file_number) != alive_files.end())
                    continue;
                if (file_type != FileType::kBlobFile &&
                    file_type != FileType::kDescriptorFile)
                    continue;
                ROCKS_LOG_INFO(db_options_.info_log,
                               "Titan recovery delete obsolete file %s.", f.c_str());
                env_->DeleteFile(dirname_ + "/" + f);
            }

            return Status::OK();
        }
        // 打开file_number对应的Manifest文件，保存当前的快照，同时将CURRENT设置为新的manifest文件
        // 这个地方其实不是很懂这个file_number对应的是一个新的文件还是旧的文件，也许并没有影响
        Status BlobFileSet::OpenManifest(uint64_t file_number)
        {
            Status s;
            // 按照MANIFEST-{file_number}的格式拼接得到文件名
            auto file_name = DescriptorFileName(dirname_, file_number);
            std::unique_ptr<WritableFileWriter> file;
            {
                std::unique_ptr<WritableFile> f;
                s = env_->NewWritableFile(file_name, &f, env_options_);
                if (!s.ok())
                    return s;
                file.reset(new WritableFileWriter(std::move(f), file_name, env_options_));
            }

            manifest_.reset(new log::Writer(std::move(file), 0, false));

            // Saves current snapshot
            s = WriteSnapshot(manifest_.get());
            if (s.ok())
            {
                ImmutableDBOptions ioptions(db_options_);
                s = SyncTitanManifest(env_, stats_, &ioptions, manifest_->file());
            }
            if (s.ok())
            {
                // Makes "CURRENT" file that points to the new manifest file.
                s = SetCurrentFile(env_, dirname_, file_number, nullptr);
            }

            if (!s.ok())
            {
                manifest_.reset();
                obsolete_manifests_.emplace_back(file_name);
            }
            return s;
        }

        Status BlobFileSet::WriteSnapshot(log::Writer *log)
        {
            Status s;
            // Saves global information
            {
                VersionEdit edit;
                edit.SetNextFileNumber(next_file_number_.load());
                std::string record;
                edit.EncodeTo(&record);
                s = log->AddRecord(record);
                if (!s.ok())
                    return s;
            }
            // Saves column families information
            for (auto &it : this->column_families_)
            {
                VersionEdit edit;
                edit.SetColumnFamilyID(it.first);
                for (auto &file : it.second->files_)
                {
                    // skip obsolete file
                    if (file.second->is_obsolete())
                    {
                        continue;
                    }
                    edit.AddBlobFile(file.second);
                }
                std::string record;
                edit.EncodeTo(&record);
                s = log->AddRecord(record);
                if (!s.ok())
                    return s;
            }
            return s;
        }
        // 写日志然后对EditCollector中的edit进行apply
        Status BlobFileSet::LogAndApply(VersionEdit &edit)
        {
            TEST_SYNC_POINT("BlobFileSet::LogAndApply");
            // TODO(@huachao): write manifest file unlocked
            std::string record;
            edit.SetNextFileNumber(next_file_number_.load());
            edit.EncodeTo(&record);

            EditCollector collector;
            Status s = collector.AddEdit(edit);
            if (!s.ok())
                return s;
            s = collector.Seal(*this);
            if (!s.ok())
                return s;
            s = manifest_->AddRecord(record);
            if (!s.ok())
                return s;

            ImmutableDBOptions ioptions(db_options_);
            s = SyncTitanManifest(env_, stats_, &ioptions, manifest_->file());
            if (!s.ok())
                return s;
            return collector.Apply(*this);
        }
        // 将参数column_families加入BlobFileSet中的`column_families_`成员
        void BlobFileSet::AddColumnFamilies(
            const std::map<uint32_t, TitanCFOptions> &column_families)
        {
            for (auto &cf : column_families)
            {
                auto file_cache = std::make_shared<BlobFileCache>(db_options_, cf.second,
                                                                  file_cache_, stats_);
                auto blob_storage = std::make_shared<BlobStorage>(
                    db_options_, cf.second, cf.first, file_cache, stats_);
                if (stats_ != nullptr)
                {
                    stats_->InitializeCF(cf.first, blob_storage);
                }
                column_families_.emplace(cf.first, blob_storage);
            }
        }
        // 每一个column_families可能对应了多个blob文件？所以需要对每一个进行测试，将column_families中包含的cf去掉
        Status BlobFileSet::DropColumnFamilies(
            const std::vector<uint32_t> &column_families,
            SequenceNumber obsolete_sequence)
        {
            Status s;
            for (auto &cf_id : column_families)
            {
                auto it = column_families_.find(cf_id);
                if (it != column_families_.end())
                {
                    VersionEdit edit;
                    edit.SetColumnFamilyID(it->first);
                    for (auto &file : it->second->files_)
                    {
                        if (!file.second->is_obsolete())
                        {
                            ROCKS_LOG_INFO(db_options_.info_log,
                                           "Titan add obsolete file [%" PRIu64 "]",
                                           file.second->file_number());
                            edit.DeleteBlobFile(file.first, obsolete_sequence);
                        }
                    }
                    s = LogAndApply(edit);
                    if (!s.ok())
                        return s;
                }
                else
                {
                    ROCKS_LOG_ERROR(db_options_.info_log, "column %u not found for drop\n",
                                    cf_id);
                    return Status::NotFound("invalid column family");
                }
                obsolete_columns_.insert(cf_id);
            }
            return s;
        }
        // 完全从磁盘上抹除某一个cf，但是实际上也只是进行了标注...？
        Status BlobFileSet::MaybeDestroyColumnFamily(uint32_t cf_id)
        {
            obsolete_columns_.erase(cf_id);
            auto it = column_families_.find(cf_id);
            if (it != column_families_.end())
            {
                it->second->MarkDestroyed();
                if (it->second->MaybeRemove())
                {
                    column_families_.erase(it);
                }
                return Status::OK();
            }
            ROCKS_LOG_ERROR(db_options_.info_log, "column %u not found for destroy\n",
                            cf_id);
            return Status::NotFound("invalid column family");
        }
        // 将范围的删除操作整合在一个edit中，通过调用LogAndApply来完成
        Status BlobFileSet::DeleteBlobFilesInRanges(uint32_t cf_id,
                                                    const RangePtr *ranges, size_t n,
                                                    bool include_end,
                                                    SequenceNumber obsolete_sequence)
        {
            auto it = column_families_.find(cf_id);
            if (it != column_families_.end())
            {
                VersionEdit edit;
                edit.SetColumnFamilyID(cf_id);

                std::vector<uint64_t> files;
                Status s = it->second->GetBlobFilesInRanges(ranges, n, include_end, &files);
                if (!s.ok())
                    return s;
                for (auto file_number : files)
                {
                    edit.DeleteBlobFile(file_number);
                }
                s = LogAndApply(edit);
                return s;
            }
            ROCKS_LOG_ERROR(db_options_.info_log,
                            "column %u not found for delete blob files in ranges\n",
                            cf_id);
            return Status::NotFound("invalid column family");
        }
        // 没太看懂
        void BlobFileSet::GetObsoleteFiles(std::vector<std::string> *obsolete_files,
                                           SequenceNumber oldest_sequence)
        {
            for (auto it = column_families_.begin(); it != column_families_.end();)
            {
                auto &cf_id = it->first;
                auto &blob_storage = it->second;
                // In the case of dropping column family, obsolete blob files can be deleted
                // only after the column family handle is destroyed.
                if (obsolete_columns_.find(cf_id) != obsolete_columns_.end())
                {
                    ++it;
                    continue;
                }

                blob_storage->GetObsoleteFiles(obsolete_files, oldest_sequence);

                // Cleanup obsolete column family when all the blob files for that are
                // deleted.
                if (blob_storage->MaybeRemove())
                {
                    it = column_families_.erase(it);
                    continue;
                }
                ++it;
            }

            obsolete_files->insert(obsolete_files->end(), obsolete_manifests_.begin(),
                                   obsolete_manifests_.end());
            obsolete_manifests_.clear();
        }

    } // namespace titandb
} // namespace rocksdb
