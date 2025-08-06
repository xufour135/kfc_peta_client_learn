#pragma 0 
#include <sys/time.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <ctime>

#include <algorithm>
#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
#include "peta/cache_option.h"
#include "peta/object_counter.h"

namespace peta {
    static inline uint16_t get_usec_ts() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return static_cast<uint16_t>(tv.tv_sec * 1000000 + tv.tv_usec);
    } 

    //K: key, V:value ValueDeleter: value deleter function, Hasher: hash function
    template <typename K, typename V, typename ValueDeleter, typename Hasher> 
    // LruHashMap is a thread-safe LRU cache with a fixed size, which evicts the least recently used items when the cache is full.
    class LruHashMap {
        struct ValueNode;
        struct Node;
    
        public: 
            LruHashMap(const CacheOption& config); // 配置文件
            ~LruHashMap();
            
            size_t size() {return node_count_.load(std::memory_order_relaxed);};

            size_t capacity() {return cache_size_;};

            bool Empty() {return size()==0;}

            const V* Get(const K& key) {
                bool expired = false;
                return Get(key, expired, get_usec_ts());
            };

            const V* Get(const K& key, bool& expired, uint16_t now) {};
            
            void Add(const K& key, const V& value);
            const V* AddInternal(const K& key, const V* value);
            
            inline void DelayedReleaseValue(ValueNode* value_node) {
                auto* head = delayed_release_node.load(std::memory_order_relaxed);
                value->release_list_next = head;
                while(!delayed_release_node.compare_exchange_weak(head, value_node, std::memory_order_relaxed)) ;
            }

            inline void DelayedReturnNode (Node* node){
                auto* value = node->value_node.exchange(nullptr, std::memory_order_relaxed);
                if(value) {
                    DelayedReleaseValue(value);
                }
                return_object(node);
            }
            
            inline void ReturnNode(Node* node) {
                auto* value = node->value.exchange(nullptr, std::memory_order_relaxed);
                if (value) {
                    return_object(value);
                }
                return_object(node);
            }

            void Evict(); // 淘汰
            void Erase(const K& key);
            void clear();
        private: 
         
            struct ValueNode {
                ValueNode* release_list_next;// 
                const V* value = nullptr;
                uint16_t deadline;
                const V* v() const{
                    return value;
                }
                void reset() {
                    //   ValueDeleter()(const_cast<V*>(value));
                    value = nullptr;
                    release_list_next = nullptr;
                }
                void init_deadline(uint16_t ttl, uint16_t now) {
                    if(now ==0) {
                        now = get_usec_ts();
                    }
                    uint16_t t = std::log(ttl);
                    uint16_t r = ttl +  ((int64_t)butil::fast_rand_less_than(t) - t / 2);// randomize the deadline limit [-t/2, t/2]
                    deadline = now + r;
                }

                bool valid(uint16_t now) const { // check if the value is still valid
                    if(now == 0) {
                        now = get_usec_ts();
                    }
                    return deadline > now;
                }

            }
            
            struct Node {
                K key;
                Node* next;
                std::atomic<ValueNode*> value_node;
                std::atomic<uint16_t> ts;
                // 0: to-be-removed, 1: expired, 2: value-is-writing, > 2: timestamp_us 读取时的时间戳
                void reset() {
                    value_node.store(nullptr, std::memory_order_relaxed);
                    next = nullptr;
                }
            };
            
            // 释放value 节点 可能会doublefree
            inline void releaase_value_node(ValueNode* value_node) {
                if(value_node->release_list_next) {
                    releaase_value_node(value_node->release_list_next);
                }
                value_node->reset();
                delete value_node;
            };
            // 释放node 节点 
            inline void release_node(Node* node) {
                ValueNode* value = node->value_node.exchange(nullptr, std::memory_order_acquire);
                if(value){
                    releaase_value_node(value);
                }
                node->reset();
                delete node;
            };
            
            inline size_t ComputeHash(const K& key) {
                return hasher_(key)%bucket_size_;
            };
            
            size_t bucket_size_;
            size_t cache_size_;  
            uint16_t ttl_us_;
            std::atomic<size_t> node_count_;
            std::vector<std::atomic<Node*>> buckets_;
            Hasher hasher_;

            std::vector<Node*>evicted_nodes_;
            // delete value in second evict round
            std::atomic<ValueNode*> delayed_release_node; 
            // epoch delete values
            ValueNode* ready_to_release_node;

    };
    
    template <typename K, typename V, typename ValueDeleter, typename Hasher> 
    LruHashMap<K, V, ValueDeleter, Hasher>::LruHashMap(const CacheOption& option) {
        : bucket_size_(option.bucket_size),
          cache_size_(option.capacity),
          ttl_us_(static_cast<uint16_t>(option.ttl * 1000000)),
          node_size_(0),
          buckets_(option.bucket_size),
          evicted_nodes_(),
          delayed_release_node(nullptr),
          ready_to_release_node(nullptr) {
        for(auto& bucket : buckets_) {
            bucket.store(nullptr, std::memory_order_relaxed);
        }

        // Initialize the LRU cache
        }
    };

    template <typename K, typename V, typename ValueDeleter, typename Hasher> 
    LruHashMap<K, V, ValueDeleter, Hasher>::~LruHashMap() {
        for(auto& bucket : buckets_) {
            Node* node = bucket.load(std::memory_order_relaxed);
            while (node) {
                Node* next_node = node->next;
                release_node(node);
                node = next_node;
            }
        }

        for(auto& node : evicted_nodes_) {
            release_node(node);
        }
        auto* vnode = delayed_release_node.load(std::memory_order_relaxed);
        while (vnode) {
            ValueNode* next_node = vnode->release_list_next;
            releaase_value_node(vnode);
            vnode = next_node;
        }
        vnode = ready_to_release_node;
        while (vnode) {
            ValueNode* next_node = vnode->release_list_next;
            releaase_value_node(vnode);
            vnode = next_node;
        }
    
    }

    template <typename K, typename V, typename ValueDeleter, typename Hasher> 
    const V* LruHashMap<K, V, ValueDeleter, Hasher>::Get(const K& key, bool& expired, uint16_t now) {
        expired = false;
        size_t bucket_index = ComputeHash(key);
        Node* node = buckets_[bucket_index].load(std::memory_order_relaxed);
        
        while (node) {
            if (node->key == key) {
                auto ts = node.ts.load(std::memory_order_relaxed);
                if(ts > 2 ) {
                    auto* vnode = node->value_node.load(std::memory_order_relaxed);
                    if(vnode->valid(now)) {
                       node->ts.compare_exchange_strong(ts, now, std::memory_order_relaxed);
                       
                       auto* vnode = node->value.load(std::memory_order_relaxed);
                       return vnode->v(); 
                    } else {
                        expired = true;
                        // mark as expired
                        node->ts.compare_exchange_strong(ts, 1, std::memory_order_relaxed);
                        return nullptr;
                    }
                } else if(ts == 1) {
                    expired = true;
                    return nullptr;
                } else {
                    return nullptr;
                }
            }
            node = node->next;
        }
        return nullptr;
    }
    
    template <typename K, typename V, typename ValueDeleter, typename Hasher> 
    void LruHashMap<K, V, ValueDeleter, Hasher>::Add(const K& key, const V& value) {
       while(AddInternal(key, value) != value) {
            continue;
        }
    }

    template <typename K, typename V, typename ValueDeleter, typename Hasher>
    const V* LruHashMap<K, V, ValueDeleter, Hasher>::AddInternal(const K& key, const V* value) {
        size_t bucket_index = ComputeHash(key);
        Node* head = buckets_[bucket_index].load(std::memory_order_relaxed);
        Node* prev_node = nullptr;
        auto curr_ts = get_usec_ts();
        const V* rc = nullptr;
        do {
            rc = nullptr;
            Node* node = head;
            while(node != nullptr) {
                if(node->key == key) {
                    auto ts = node->ts.load(std::memory_order_relaxed);
                    if(ts > 2 || ts == 1) {
                        if(node->ts.compare_exchange_strong(ts, 2, std::memory_order_relaxed)) {
                            auto* vnode = get_object<ValueNode>();
                            vnode->value = value;
                            vnode->init_deadline(ttl_us_, curr_ts);
                            auto* pre = node->value_node.exchange(vnode, std::memory_order_relaxed);
                            if(prev_node !=nullptr) {
                                prev_node->value.load(std::memory_order_relaxed)->release_list_next = nullptr;
                            }
                            DelayedReleaseValue(pre);
                            auto cur_ts = get_usec_ts();
                            node->ts.store(cur_ts, std::memory_order_relaxed);
                            rc = value;
                        }
                    } else if(ts == 0 ) {
                        break;
                    } else {
                        rc = nullptr;
                    }

                    if(prev_node !=nullptr) {
                        prev_node->value_node.load(std::memory_order_relaxed)->value = nullptr;
                        DelayedReturnNode(prev_node);
                    } 
                    return rc;
                }
                node = node->next;
            }
            if(prev_node == nullptr) {
                prev_node = get_object<Node>();
                prev_node->key = key;
                auto* new_value_node = get_object<ValueNode>();
                new_value_node->value = value;
                new_value_node->init_deadline(ttl_us_, curr_ts);
                prev_node->value_node.store(new_value_node, std::memory_order_relaxed);
            } 
            prev_node->ts.store(curr_ts, std::memory_order_relaxed);
            prev_node->next = head;
            rc = prev_node->value_node.load(std::memory_order_relaxed)->value;
        }while(!buckets_[bucket_index].compare_exchange_strong(head, prev_node, std::memory_order_acq_rel));
        node_count_.fetch_add(1, std::memory_order_relaxed);
        return rc;
    }
    template <typename K, typename V, typename ValueDeleter, typename Hasher>
    void LruHashMap<K, V, ValueDeleter, Hasher>::Erase(const K& key) {
        size_t bucket_index = ComputeHash(key);
        Node* node = buckets_[bucket_index].load(std::memory_order_relaxed);
        while (node) {
            if (node->key == key) {
                auto ts = node->ts.load(std::memory_order_relaxed);
                if(ts > 2) {
                    while(ts > 2 && !node->ts.compare_exchange_strong(ts, 1, std::memory_order_relaxed));
                    return ;
                }
                // 0 del /1 expired /2 writing
                
            }
            node = node->next;
        }
    }

    template <typename K, typename V, typename ValueDeleter, typename Hasher>
    void LruHashMap<K, V, ValueDeleter, Hasher>::Evict() {
        thread_local int counter = 0;
        counter +=1;

        for(auto* node: evicted_nodes_) {
            ReturnNode(node);
        }
        evicted_nodes_.clear();

            // size_t node_count = node_count_.load(std::memory_order_relaxed);
            // PETA_LOG(INFO) << "counter:" << counter << " LruHashMap:" << option_.name
            //      << " evict node_count:" << node_count
            //      << " cache_size:" << cache_size_;
        
            // 1st, traverse all nodes and collect their timestamps
            // remove expired nodes by tts
            std::vector<uint16_t> node_ts;
            for(auto& bucket : buckets_) {
                Node* node = bucket.load(std::memory_order_relaxed);
                while (node) {
                    auto ts = node->ts.load(std::memory_order_relaxed);
                    if(ts != 2) {
                        node_ts.push_back(ts);
                    }
                    node = node->next;
                }
            }
            std::sort(node_ts.begin(), node_ts.end());
            size_t evict_ts = 10;
            int evict_count = -1;
            size_t traversed_node_count = node_ts.size();
            if(traversed_node_count > cache_size_) {
                evict_count = traversed_node_count - cache_size_ ;
                evict_ts = node_ts[evict_count];
            }
            else {
                if(node_ts[0] > evict_ts) {
                    return ;
                }
            }
            //   PETA_LOG(INFO) << "counter:" << counter << " LruHashMap:" << option_.name
            //      << " evict ts:" << evict_ts << " begin:" << nodes_ts[0]
            //      << " end:" << nodes_ts.back() << " count:" << evict_count;
    
            // 2nd, remove node ts < evict_ts
            for(auto& bucket : buckets_) {
                auto* head = bucket.load(std::memory_order_relaxed);
                auto* node = head;
                Node* prev = nullptr;
                while (node) { 
                    auto ts = node->ts.load(std::memory_order_relaxed);
                    if(ts < evict_ts && ts != 2) {
                       if(node->ts.compare_exchange_strong(ts, 0, std::memory_order_relaxed)) {
                          if(node == head) {
                            if(bucket.compare_exchange_strong(head, node->next, std::memory_order_acq_rel)) {
                                evicted_nodes_.push_back(node);
                                head = node->next;
                            } else {
                                auto* tmp = head;
                                while(tmp !=node) {
                                    prev = tmp;
                                    tmp = tmp->next;
                                }
                                continue;   
                            }
                          } else {
                            prev->next = node->next;
                            evicted_nodes_.push_back(node);
                            node = node->next;
                            continue;
                          }  
                        } 
                    } 
                    prev = node;
                    node = node->next;
                }
            }

            auto nc = node_count_.fetch_sub(evicted_nodes_.size(), std::memory_order_relaxed);
            // cache_size_.set_value(nc);

            auto* vnode = ready_to_release_node;
            while(vnode) {
                auto* next = vnode->release_list_next;
                return_object(vnode);
                vnode = next;
            } 

            ready_to_release_node = delayed_release_node.exchange(nullptr, std::memory_order_acquire);
            //   PETA_LOG(INFO) << "counter:" << counter << " LruHashMap:" << option_.name
            //      << " evict count:" << evicted_nodes_.size()
            //      << " node:" << node_count_.load(std::memory_order_relaxed)
            //      << " object:" << ObjectCounter<Node>::Count() << ", "
            //      << ObjectCounter<ValueNode>::Count();
        }


} // namespace peta
