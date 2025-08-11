#include "storage/page/page_guard.h"
#include <future>
#include <memory>

namespace bustub {

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(true) {
  // 读锁 + pin + 设置不可淘汰
  frame_->rwlatch_.lock_shared();
  frame_->pin_count_.fetch_add(1, std::memory_order_relaxed);
  // 保护 replacer_ 的操作
  std::scoped_lock lk(*bpm_latch_);
  replacer_->SetEvictable(frame_->frame_id_, false);
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  // 使 that 无效，避免双重释放
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  // 释放当前持有资源
  Drop();

  // 移动接管
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  // 使 that 无效
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;

  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  // 在共享锁下写盘是安全的（阻止并发写）
  if (!frame_->is_dirty_) {
    return;
  }
  DiskRequest req{};
  req.page_id_ = page_id_;
  req.data_ = const_cast<char *>(frame_->GetData());  // 接口需要非 const 指针
  req.is_write_ = true;
  std::promise<bool> p;
  auto fut = p.get_future();
  req.callback_ = std::move(p);
  disk_scheduler_->Schedule(std::move(req));
  (void)fut.get();  // 阻塞直到 I/O 完成
  frame_->is_dirty_ = false;
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  // 原子减少 pin
  size_t prev = frame_->pin_count_.fetch_sub(1, std::memory_order_relaxed);
  // 释放读锁
  frame_->rwlatch_.unlock_shared();

  // 如果从 1 → 0，需要把该 frame 标记为可淘汰（必须在释放页锁之后拿 bpm_latch_，避免锁顺序反转）
  if (prev == 1) {
    std::scoped_lock lk(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(true) {
  // 写锁 + pin + 设置不可淘汰
  frame_->rwlatch_.lock();
  frame_->pin_count_.fetch_add(1, std::memory_order_relaxed);
  {
    std::scoped_lock lk(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, false);
  }
  // 获得写访问权即视作会修改
  frame_->is_dirty_ = true;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();

  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;

  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  if (!frame_->is_dirty_) {
    return;
  }
  // 已持有写锁，安全写盘
  DiskRequest req{};
  req.page_id_ = page_id_;
  req.data_ = frame_->GetDataMut();
  req.is_write_ = true;
  std::promise<bool> p;
  auto fut = p.get_future();
  req.callback_ = std::move(p);
  disk_scheduler_->Schedule(std::move(req));
  (void)fut.get();
  frame_->is_dirty_ = false;
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  // 原子减少 pin
  size_t prev = frame_->pin_count_.fetch_sub(1, std::memory_order_relaxed);
  // 释放写锁
  frame_->rwlatch_.unlock();

  if (prev == 1) {
    std::scoped_lock lk(*bpm_latch_);
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
