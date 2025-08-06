#include "butil/object_pool.h"
#include <atomic>
template <class T>
namespace peta {

    struct ObjectCounter {
        static ObjectCounter* instance() {
            static ObjectCounter counter;
            return &counter;
        }
        static inline void Get(){
            instance()->count.fetch_add(1, std::memory_order_relaxed);
        }
        static inline void Reset(){
            instance()->count.fetch_sub(1, std::memory_order_relaxed);
        }
        static inline int Count(){
            return instance()->count.load(std::memory_order_relaxed);
        }

        std::atomic<int> count{0};
    };

    template<class T>
    static inline T* get_object() {
        ObjectCounter<T>::Get();
        return butil::get_object<T>();
    }

    template<class T>
    static inline void return_object(T* obj) {
        ObjectCounter<T>::Reset();
        obj->Reset();
        butil::return_object<T>(obj);
    }
} // namespace peta