#include "blob_gc.h"

namespace rocksdb
{
    namespace titandb
    {

        /**
         * 可以基本认为是一个数据类，几乎所有的成员都只有get/set方法
         * @param blob_files
         * @param _titan_cf_options
         * @param need_trigger_next
         */
        BlobGC::BlobGC(std::vector<std::shared_ptr<BlobFileMeta>> &&blob_files,
                       TitanCFOptions &&_titan_cf_options, bool need_trigger_next)
            : inputs_(blob_files),
              titan_cf_options_(std::move(_titan_cf_options)),
              trigger_next_(need_trigger_next)
        {
            MarkFilesBeingGC();
        }

        BlobGC::~BlobGC() {}

        void BlobGC::SetColumnFamily(ColumnFamilyHandle *cfh) { cfh_ = cfh; }

        ColumnFamilyData *BlobGC::GetColumnFamilyData()
        {
            auto *cfhi = reinterpret_cast<ColumnFamilyHandleImpl *>(cfh_);
            return cfhi->cfd();
        }

        /**
         * 将blob_file加入outputs_队列
         * @param blob_file
         */
        void BlobGC::AddOutputFile(BlobFileMeta *blob_file)
        {
            outputs_.push_back(blob_file);
        }

        /**
         * 更改inputs_中所有文件的状态为kGCBegin
         */
        void BlobGC::MarkFilesBeingGC()
        {
            for (auto &f : inputs_)
            {
                f->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
            }
        }
        /**
         * 标记inputs_中所以文件为kGCCompleted
         * 以及outputs_中所有文件为kGCCompleted
         */
        void BlobGC::ReleaseGcFiles()
        {
            for (auto &f : inputs_)
            {
                f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
            }

            for (auto &f : outputs_)
            {
                f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
            }
        }

    } // namespace titandb
} // namespace rocksdb
