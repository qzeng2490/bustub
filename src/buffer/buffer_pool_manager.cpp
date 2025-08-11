#include "buffer/buffer_pool_manager.h"

namespace bustub {

FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

auto FrameHeader::GetData() const -> const char * { return data_.data(); }

auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);
  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t {
  // 单调自增的线程安全分配
  return next_page_id_.fetch_add(1, std::memory_order_relaxed);
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock lk(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    auto &fh = frames_.at(fid);
    // 如仍被使用，则删除失败
    if (fh->pin_count_.load(std::memory_order_relaxed) > 0) {
      return false;
    }
    // 从 page_table_ 移除
    page_table_.erase(it);
    // 清理 replacer 状态并归还空闲列表
    replacer_->SetEvictable(fid, false);  // 即将重置，不参与置换
    fh->Reset();
    free_frames_.push_back(fid);
  }

  // 释放磁盘上的页面空间（按你的工程接口替换调用位置）
  // 如果 DiskScheduler 暴露的是 DiskManager::DeallocatePage，可改为直接调用。
  if (disk_scheduler_) {
    DiskRequest req{};
    req.page_id_ = page_id;
    req.is_write_ = false;
    req.is_deallocate_ = true;  // 若你的 DiskRequest 不含该字段，请改为调用相应 API
    std::promise<bool> p;
    auto fut = p.get_future();
    req.callback_ = std::move(p);
    disk_scheduler_->Schedule(std::move(req));
    (void)fut.get();
  }

  return true;
}

static auto FindPageIdByFrame(const std::unordered_map<page_id_t, frame_id_t> &pt, frame_id_t fid)
    -> std::optional<page_id_t> {
  for (const auto &kv : pt) {
    if (kv.second == fid) {
      return kv.first;
    }
  }
  return std::nullopt;
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type)
    -> std::optional<WritePageGuard> {
  std::unique_lock lk(*bpm_latch_);

  // 命中
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    frame_id_t fid = it->second;
    replacer_->RecordAccess(fid, access_type);
    auto guard = WritePageGuard(page_id, frames_[fid], replacer_, bpm_latch_, disk_scheduler_);
    return std::make_optional<WritePageGuard>(std::move(guard));
  }

  frame_id_t use_fid = -1;
  if (!free_frames_.empty()) {
    use_fid = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // 原来是: if (!replacer_->Evict(&use_fid)) { return std::nullopt; }
    auto victim_opt = replacer_->Evict();
    if (!victim_opt.has_value()) {
      return std::nullopt;
    }
    use_fid = victim_opt.value();

    // 找出被淘汰帧对应的旧页
    auto victim_pid_opt = FindPageIdByFrame(page_table_, use_fid);
    if (victim_pid_opt.has_value()) {
      auto &fh = frames_[use_fid];
      if (fh->is_dirty_) {
        DiskRequest req{};
        req.page_id_ = victim_pid_opt.value();
        req.data_ = fh->GetDataMut();
        req.is_write_ = true;
        std::promise<bool> p;
        auto fut = p.get_future();
        req.callback_ = std::move(p);
        disk_scheduler_->Schedule(std::move(req));
        (void)fut.get();
        fh->is_dirty_ = false;
      }
      page_table_.erase(victim_pid_opt.value());
    }
  }

  // ... 其余逻辑保持不变
  auto &fh = frames_[use_fid];
  fh->Reset();
  {
    DiskRequest req{};
    req.page_id_ = page_id;
    req.data_ = fh->GetDataMut();
    req.is_write_ = false;
    std::promise<bool> p;
    auto fut = p.get_future();
    req.callback_ = std::move(p);
    disk_scheduler_->Schedule(std::move(req));
    (void)fut.get();
  }
  page_table_[page_id] = use_fid;
  replacer_->RecordAccess(use_fid, access_type);
  replacer_->SetEvictable(use_fid, false);
  auto guard = WritePageGuard(page_id, fh, replacer_, bpm_latch_, disk_scheduler_);
  return std::make_optional<WritePageGuard>(std::move(guard));
}

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type)
    -> std::optional<ReadPageGuard> {
  std::unique_lock lk(*bpm_latch_);

  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    frame_id_t fid = it->second;
    replacer_->RecordAccess(fid, access_type);
    auto guard = ReadPageGuard(page_id, frames_[fid], replacer_, bpm_latch_, disk_scheduler_);
    return std::make_optional<ReadPageGuard>(std::move(guard));
  }

  frame_id_t use_fid = -1;
  if (!free_frames_.empty()) {
    use_fid = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // 原来是: if (!replacer_->Evict(&use_fid)) { return std::nullopt; }
    auto victim_opt = replacer_->Evict();
    if (!victim_opt.has_value()) {
      return std::nullopt;
    }
    use_fid = victim_opt.value();

    if (auto victim_pid_opt = FindPageIdByFrame(page_table_, use_fid); victim_pid_opt.has_value()) {
      auto &fh = frames_[use_fid];
      if (fh->is_dirty_) {
        DiskRequest req{};
        req.page_id_ = victim_pid_opt.value();
        req.data_ = fh->GetDataMut();
        req.is_write_ = true;
        std::promise<bool> p;
        auto fut = p.get_future();
        req.callback_ = std::move(p);
        disk_scheduler_->Schedule(std::move(req));
        (void)fut.get();
        fh->is_dirty_ = false;
      }
      page_table_.erase(victim_pid_opt.value());
    }
  }

  // ... 其余逻辑保持不变
  auto &fh = frames_[use_fid];
  fh->Reset();
  {
    DiskRequest req{};
    req.page_id_ = page_id;
    req.data_ = fh->GetDataMut();
    req.is_write_ = false;
    std::promise<bool> p;
    auto fut = p.get_future();
    req.callback_ = std::move(p);
    disk_scheduler_->Schedule(std::move(req));
    (void)fut.get();
  }
  page_table_[page_id] = use_fid;
  replacer_->RecordAccess(use_fid, access_type);
  replacer_->SetEvictable(use_fid, false);
  auto guard = ReadPageGuard(page_id, fh, replacer_, bpm_latch_, disk_scheduler_);
  return std::make_optional<ReadPageGuard>(std::move(guard));
}

auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  std::scoped_lock lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t fid = it->second;
  auto &fh = frames_[fid];

  if (!fh->is_dirty_) {
    return true;
  }

  // 不加页锁，按注释要求小心时序：发起写后即清除 dirty 位
  DiskRequest req{};
  req.page_id_ = page_id;
  req.data_ = fh->GetDataMut();
  req.is_write_ = true;
  std::promise<bool> p;
  auto fut = p.get_future();
  req.callback_ = std::move(p);
  disk_scheduler_->Schedule(std::move(req));
  (void)fut.get();
  fh->is_dirty_ = false;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::shared_ptr<FrameHeader> fh;
  {
    std::scoped_lock lk(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    fh = frames_[it->second];
  }

  // 对该页加写锁，保证一致性
  std::unique_lock wl(fh->rwlatch_);
  if (!fh->is_dirty_) {
    return true;
  }

  DiskRequest req{};
  req.page_id_ = page_id;
  req.data_ = fh->GetDataMut();
  req.is_write_ = true;
  std::promise<bool> p;
  auto fut = p.get_future();
  req.callback_ = std::move(p);
  disk_scheduler_->Schedule(std::move(req));
  (void)fut.get();
  fh->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPagesUnsafe() {
  std::scoped_lock lk(*bpm_latch_);
  for (const auto &kv : page_table_) {
    page_id_t pid = kv.first;
    frame_id_t fid = kv.second;
    auto &fh = frames_[fid];
    if (!fh->is_dirty_) {
      continue;
    }
    DiskRequest req{};
    req.page_id_ = pid;
    req.data_ = fh->GetDataMut();
    req.is_write_ = true;
    std::promise<bool> p;
    auto fut = p.get_future();
    req.callback_ = std::move(p);
    disk_scheduler_->Schedule(std::move(req));
    (void)fut.get();
    fh->is_dirty_ = false;
  }
}

void BufferPoolManager::FlushAllPages() {
  // 逐页安全刷盘：遍历时只持 bpm_latch_ 收集目标，再逐个加写锁 flush
  std::vector<std::pair<page_id_t, std::shared_ptr<FrameHeader>>> targets;
  {
    std::scoped_lock lk(*bpm_latch_);
    targets.reserve(page_table_.size());
    for (const auto &kv : page_table_) {
      targets.emplace_back(kv.first, frames_[kv.second]);
    }
  }
  for (auto &[pid, fh] : targets) {
    std::unique_lock wl(fh->rwlatch_);
    if (!fh->is_dirty_) {
      continue;
    }
    DiskRequest req{};
    req.page_id_ = pid;
    req.data_ = fh->GetDataMut();
    req.is_write_ = true;
    std::promise<bool> p;
    auto fut = p.get_future();
    req.callback_ = std::move(p);
    disk_scheduler_->Schedule(std::move(req));
    (void)fut.get();
    fh->is_dirty_ = false;
  }
}

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  auto &fh = frames_[it->second];
  return std::make_optional<size_t>(fh->pin_count_.load(std::memory_order_relaxed));
}

}  // namespace bustub
