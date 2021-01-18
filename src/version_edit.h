#pragma once

#include <set>

#include "blob_format.h"
#include "rocksdb/slice.h"

#include <inttypes.h>

namespace rocksdb
{
    namespace titandb
    {

        enum Tag
        {
            // QUES: 其实不知道为啥不直接用顺序的，而是需要避开2-9
            kNextFileNumber = 1,
            kColumnFamilyID = 10,
            kAddedBlobFile = 11,
            kDeletedBlobFile = 12, // Deprecated, leave here for backward compatibility
            kAddedBlobFileV2 = 13, // Comparing to kAddedBlobFile, it newly includes
                                   // smallest_key and largest_key of blob file
        };

        class VersionEdit
        {
        public:
            void SetNextFileNumber(uint64_t v)
            {
                has_next_file_number_ = true;
                next_file_number_ = v;
            }

            void SetColumnFamilyID(uint32_t v) { column_family_id_ = v; }

            void AddBlobFile(std::shared_ptr<BlobFileMeta> file)
            {
                added_files_.push_back(file);
            }

            void DeleteBlobFile(uint64_t file_number,
                                SequenceNumber obsolete_sequence = 0)
            {
                deleted_files_.emplace_back(std::make_pair(file_number, obsolete_sequence));
            }

            void EncodeTo(std::string *dst) const;
            Status DecodeFrom(Slice *src);

            friend bool operator==(const VersionEdit &lhs, const VersionEdit &rhs);

        private:
            friend class BlobFileSet;
            friend class VersionTest;
            friend class EditCollector;

            bool has_next_file_number_{false};
            uint64_t next_file_number_{0};
            uint32_t column_family_id_{0};

            std::vector<std::shared_ptr<BlobFileMeta>> added_files_;
            // QUES: 为何deleted_files_和added_files_的内容不一样？
            // 因为删除过的文件只有SequenceNumber吗
            // 但是实际上Encode的时候又说SequenceNumber不是一个需要持久化的内容
            std::vector<std::pair<uint64_t, SequenceNumber>> deleted_files_;
        };

    } // namespace titandb
} // namespace rocksdb
