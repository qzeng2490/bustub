//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"

#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // 启动后台工作线程
  background_thread_ = std::thread([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // 通过向队列中放入 nullopt 来通知后台线程停止
  request_queue_.Put(std::nullopt);
  // 等待后台线程结束
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // 将请求包装在 optional 中并放入队列
  request_queue_.Put(std::make_optional(std::move(r)));
}

void DiskScheduler::StartWorkerThread() {
  // 持续处理队列中的请求，直到收到停止信号
  while (true) {
    // 从队列中获取请求
    auto optional_request = request_queue_.Get();
    
    // 如果收到 nullopt，表示需要停止工作线程
    if (!optional_request.has_value()) {
      break;
    }
    
    // 获取实际的请求
    auto request = std::move(optional_request.value());
    
    // 根据请求类型调用相应的磁盘管理操作
    bool success = false;
    try {
      if (request.is_write_) {
        // 写操作
        disk_manager_->WritePage(request.page_id_, request.data_);
        success = true;
      } else {
        // 读操作
        disk_manager_->ReadPage(request.page_id_, request.data_);
        success = true;
      }
    } catch (const Exception &e) {
      // 如果操作失败，success 保持为 false
      success = false;
    }
    
    // 通过 promise 通知请求发起者操作已完成
    request.callback_.set_value(success);
  }
}

}  // namespace bustub