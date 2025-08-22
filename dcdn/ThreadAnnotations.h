// ThreadAnnotations.h
#pragma once
#include <mutex>
#if defined(__clang__)
  #define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
  #define THREAD_ANNOTATION_ATTRIBUTE__(x)  // no-op on non-clang
#endif

// Capabilities (locks, thread tokens)
#define CAPABILITY(x)                   THREAD_ANNOTATION_ATTRIBUTE__(capability(x))
#define SCOPED_CAPABILITY               THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)
#define GUARDED_BY(x)                  THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#define PT_GUARDED_BY(x)               THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define ACQUIRE(...)                   THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))
#define ACQUIRE_SHARED(...)            THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))
#define RELEASE(...)                   THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))
#define RELEASE_SHARED(...)            THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))
#define TRY_ACQUIRE(...)               THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))
#define TRY_ACQUIRE_SHARED(...)        THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define REQUIRES(...)                  THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))
#define REQUIRES_SHARED(...)           THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))
#define EXCLUDES(...)                  THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define RETURN_CAPABILITY(x)           THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))
#define EXCLUSIVE_LOCKS_REQUIRED(...)  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))
#define SHARED_LOCKS_REQUIRED(...)     THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))
#define LOCKS_EXCLUDED(...)            THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define NO_THREAD_SAFETY_ANALYSIS      THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

namespace dcdn {
class CAPABILITY("mutex") AnnotatedMutex {
public:
    AnnotatedMutex() = default;

    void lock()                        ACQUIRE()             { mu_.lock(); }
    void unlock()                      RELEASE()             { mu_.unlock(); }
    bool try_lock()                    TRY_ACQUIRE(true)     { return mu_.try_lock(); }

    std::mutex&       native_handle()  RETURN_CAPABILITY(*this) { return mu_; }
    const std::mutex& native_handle()  const RETURN_CAPABILITY(*this) { return mu_; }

    class SCOPED_CAPABILITY Guard {
    public:
        explicit Guard(AnnotatedMutex& m) ACQUIRE(m) : m_(m), lk_(m.mu_) {}
        ~Guard() RELEASE() { /* no-op; rely on lk_ RAII */ }

        Guard(const Guard&)            = delete;
        Guard& operator=(const Guard&) = delete;
        Guard(Guard&&)                 = delete;
        Guard& operator=(Guard&&)      = delete;

        std::unique_lock<std::mutex>& as_unique_lock() { return lk_; }

    private:
        AnnotatedMutex& m_;
        std::unique_lock<std::mutex> lk_;
    };

private:
    std::mutex mu_;
};

// download manager thread capability
class CAPABILITY("dm-thread") DmLoopThreadCap {
public:
    void Acquire() ACQUIRE() {}
    void Release() RELEASE() {}

    class SCOPED_CAPABILITY Guard {
    public:
        explicit Guard(DmLoopThreadCap& cap) ACQUIRE(cap) : cap_(cap) {
            cap_.Acquire();
        }
        ~Guard() RELEASE() {
            cap_.Release();
        }

        Guard(const Guard&)            = delete;
        Guard& operator=(const Guard&) = delete;
        Guard(Guard&&)                 = delete;
        Guard& operator=(Guard&&)      = delete;

    private:
        DmLoopThreadCap& cap_;
    };
};

} // namespace dcdn
